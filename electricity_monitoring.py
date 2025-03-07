#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import logging
import schedule
import pika
import mysql.connector
from mysql.connector import pooling
import requests
import queue
from datetime import datetime, timedelta
from threading import Thread, Lock, Event
from collections import defaultdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("electricity_monitoring.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 配置
CONFIG = {
    'rabbitmq': {
        'host': 'localhost',
        'port': 5672,
        'virtual_host': '/',
        'username': 'guest',
        'password': 'guest',
        'queue': 'hotel_checkin_queue'
    },
    'mysql': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'password',
        'database': 'hotel_monitoring',
        'pool_name': 'hotel_monitoring_pool',
        'pool_size': 10  # 连接池大小
    },
    'feishu': {
        'webhook_url': 'https://open.feishu.cn/open-apis/bot/v2/hook/your_webhook_token'
    },
    'monitoring': {
        'check_interval_minutes': 30,
        'checkin_abnormal_threshold': 2,       # 入住消息异常阈值
        'checkout_abnormal_threshold': 1.5,    # 离店消息异常阈值
        'continuous_alert_count': 4,
        'batch_size': 100,                    # 批处理大小
        'max_history_size': 20                # 最大历史记录数量
    },
    'message_types': {
        'checkin': '5710005',    # 入住消息类型
        'checkout': '5710013'    # 离店消息类型
    },
    'performance': {
        'consumer_count': 3      # 消费者线程数量
    }
}

# 存储房间监控状态
# 格式: {hotel_id_room_number: {state_data}}
room_monitoring_state = defaultdict(dict)

# 添加线程安全锁
room_state_lock = Lock()

# 创建停止事件
stop_event = Event()

# 消息类型常量
MSG_TYPE_CHECKIN = 'checkin'   # 入住消息
MSG_TYPE_CHECKOUT = 'checkout' # 离店消息

# 创建MySQL连接池
def create_mysql_pool():
    """创建MySQL连接池"""
    try:
        pool_config = {
            'pool_name': CONFIG['mysql']['pool_name'],
            'pool_size': CONFIG['mysql']['pool_size'],
            'host': CONFIG['mysql']['host'],
            'port': CONFIG['mysql']['port'],
            'user': CONFIG['mysql']['user'],
            'password': CONFIG['mysql']['password'],
            'database': CONFIG['mysql']['database']
        }
        
        pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
        logger.info(f"MySQL连接池创建成功，池大小={CONFIG['mysql']['pool_size']}")
        return pool
    except Exception as e:
        logger.error(f"创建MySQL连接池失败: {str(e)}")
        return None

# 全局连接池
mysql_pool = None

def get_db_connection():
    """从连接池获取数据库连接"""
    global mysql_pool
    
    if mysql_pool is None:
        mysql_pool = create_mysql_pool()
        if mysql_pool is None:
            logger.error("无法创建MySQL连接池")
            return None
    
    try:
        connection = mysql_pool.get_connection()
        return connection
    except Exception as e:
        logger.error(f"从连接池获取连接失败: {str(e)}")
        return None

def connect_rabbitmq():
    """连接到RabbitMQ并返回连接对象"""
    try:
        credentials = pika.PlainCredentials(
            CONFIG['rabbitmq']['username'],
            CONFIG['rabbitmq']['password']
        )
        parameters = pika.ConnectionParameters(
            host=CONFIG['rabbitmq']['host'],
            port=CONFIG['rabbitmq']['port'],
            virtual_host=CONFIG['rabbitmq']['virtual_host'],
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        logger.info("成功连接到RabbitMQ")
        return connection
    except Exception as e:
        logger.error(f"RabbitMQ连接失败: {str(e)}")
        return None

def send_feishu_notification(message):
    """发送飞书机器人通知"""
    try:
        webhook_url = CONFIG['feishu']['webhook_url']
        headers = {'Content-Type': 'application/json'}
        payload = {
            "msg_type": "text",
            "content": {
                "text": f"⚠️ 用电异常警报 ⚠️\n{message}"
            }
        }
        response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logger.info(f"飞书通知已发送: {message}")
            return True
        else:
            logger.error(f"飞书通知发送失败: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"飞书通知发送异常: {str(e)}")
        return False

def find_closest_electricity_reading(db, hotel_id, room_number, reference_time):
    """查找与参考时间最接近的电表读数"""
    try:
        cursor = db.cursor(dictionary=True)
        query = """
        SELECT id, reading_value, script_time 
        FROM smart_meter 
        WHERE hotel_id = %s AND room_number = %s 
        ORDER BY ABS(TIMESTAMPDIFF(SECOND, script_time, %s)) 
        LIMIT 1
        """
        cursor.execute(query, (hotel_id, room_number, reference_time))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"查询最近电表读数失败: {str(e)}")
        return None

def initialize_room_monitoring(hotel_id, room_number, reference_time, end_time, msg_type):
    """初始化房间监控状态
    
    Args:
        hotel_id: 酒店ID
        room_number: 房间号
        reference_time: 参考时间（入住消息为抵店时间，离店消息为抵店时间）
        end_time: 结束时间（入住消息为预离时间，离店消息为实际离店时间）
        msg_type: 消息类型（checkin或checkout）
    """
    try:
        db = get_db_connection()
        if not db:
            logger.error("初始化房间监控失败: 无法连接数据库")
            return False
            
        # 查询最接近参考时间的用电量记录
        result = find_closest_electricity_reading(db, hotel_id, room_number, reference_time)
        
        if result:
            initial_reading = float(result['reading_value'])
            timestamp = result['script_time']
            
            # 生成房间唯一键，加入消息类型区分不同监控
            room_key = f"{hotel_id}_{room_number}_{msg_type}"
            
            # 获取适用的阈值
            if msg_type == MSG_TYPE_CHECKIN:
                threshold = CONFIG['monitoring']['checkin_abnormal_threshold']
            else:  # MSG_TYPE_CHECKOUT
                threshold = CONFIG['monitoring']['checkout_abnormal_threshold']
            
            # 使用线程锁保护共享状态
            with room_state_lock:
                # 存储初始状态
                room_monitoring_state[room_key] = {
                    'hotel_id': hotel_id,
                    'room_number': room_number,
                    'reference_time': reference_time,
                    'end_time': end_time,
                    'initial_reading': initial_reading,
                    'last_reading': initial_reading,
                    'last_check_time': timestamp,
                    'abnormal_count': 0,
                    'readings_history': [],
                    'msg_type': msg_type,
                    'threshold': threshold
                }
            
            # 记录到数据库
            cursor = db.cursor()
            
            # 根据消息类型选择字段名
            if msg_type == MSG_TYPE_CHECKIN:
                time_field_name = "check_in_time"
                end_time_field_name = "expected_checkout_time"
            else:  # MSG_TYPE_CHECKOUT
                time_field_name = "actual_checkin_time"
                end_time_field_name = "actual_checkout_time"
            
            insert_query = f"""
            INSERT INTO electricity_monitoring 
            (hotel_id, room_number, {time_field_name}, {end_time_field_name}, initial_reading, monitoring_start_time, msg_type) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                hotel_id, 
                room_number, 
                reference_time, 
                end_time, 
                initial_reading, 
                timestamp,
                msg_type
            ))
            db.commit()
            cursor.close()
            
            logger.info(f"已初始化房间{msg_type}监控: 酒店={hotel_id}, 房间={room_number}, 初始读数={initial_reading}, 阈值={threshold}")
            return True
        else:
            logger.warning(f"未找到房间电表读数: 酒店={hotel_id}, 房间={room_number}")
            return False
    except Exception as e:
        logger.error(f"初始化房间监控异常: {str(e)}")
        return False
    finally:
        if db:
            db.close()

def get_current_electricity_reading(db, hotel_id, room_number):
    """获取当前电表读数"""
    try:
        cursor = db.cursor(dictionary=True)
        query = """
        SELECT reading_value, script_time 
        FROM smart_meter 
        WHERE hotel_id = %s AND room_number = %s 
        ORDER BY script_time DESC 
        LIMIT 1
        """
        cursor.execute(query, (hotel_id, room_number))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"获取当前电表读数失败: {str(e)}")
        return None

def record_abnormal_usage(db, hotel_id, room_number, detected_time, total_consumption, continuous_periods, msg_type):
    """记录异常用电情况到数据库"""
    try:
        cursor = db.cursor()
        insert_query = """
        INSERT INTO electricity_abnormal 
        (hotel_id, room_number, detected_time, total_consumption, continuous_periods, is_notified, msg_type) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            hotel_id, 
            room_number, 
            detected_time, 
            total_consumption, 
            continuous_periods,
            True,
            msg_type
        ))
        db.commit()
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"记录异常用电失败: {str(e)}")
        return False

def process_room_batch(room_keys_batch):
    """处理一批房间的检查任务"""
    if not room_keys_batch:
        return
    
    db = get_db_connection()
    if not db:
        logger.error("批量检查用电情况失败: 无法连接数据库")
        return
    
    try:
        for room_key in room_keys_batch:
            # 使用线程锁保护读取操作
            with room_state_lock:
                if room_key not in room_monitoring_state:
                    continue
                state = room_monitoring_state[room_key].copy()  # 复制一份避免竞态条件
            
            hotel_id = state['hotel_id']
            room_number = state['room_number']
            msg_type = state.get('msg_type', MSG_TYPE_CHECKIN)  # 默认为入住类型，兼容旧数据
            threshold = state.get('threshold', CONFIG['monitoring']['checkin_abnormal_threshold'])  # 获取阈值
            
            # 检查是否已经超过结束时间
            if 'end_time' in state and state['end_time']:
                end_time = state['end_time']
                if isinstance(end_time, str):
                    end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
                
                if datetime.now() > end_time:
                    logger.info(f"房间已超过监控结束时间，停止监控: 酒店={hotel_id}, 房间={room_number}, 类型={msg_type}")
                    with room_state_lock:
                        if room_key in room_monitoring_state:
                            del room_monitoring_state[room_key]
                    continue
            
            # 查询当前用电量
            result = get_current_electricity_reading(db, hotel_id, room_number)
            
            if result:
                current_reading = float(result['reading_value'])
                timestamp = result['script_time']
                
                # 计算差值
                last_reading = float(state['last_reading'])
                diff = current_reading - last_reading
                
                logger.info(f"房间用电检查: 酒店={hotel_id}, 房间={room_number}, 类型={msg_type}, 当前读数={current_reading}, 上次读数={last_reading}, 差值={diff}, 阈值={threshold}")
                
                # 更新状态（添加新读数和差值）
                readings_history = state['readings_history'].copy()
                readings_history.append((timestamp, current_reading, diff))
                
                # 限制历史记录数量
                max_history_size = CONFIG['monitoring']['max_history_size']
                if len(readings_history) > max_history_size:
                    readings_history = readings_history[-max_history_size:]
                
                # 检查是否超过阈值
                abnormal_count = state['abnormal_count']
                if diff > threshold:
                    abnormal_count += 1
                    logger.warning(f"检测到异常用电: 酒店={hotel_id}, 房间={room_number}, 类型={msg_type}, 差值={diff}, 连续次数={abnormal_count}")
                    
                    # 如果连续异常次数达到阈值，触发报警
                    continuous_alert_count = CONFIG['monitoring']['continuous_alert_count']
                    if abnormal_count >= continuous_alert_count:
                        # 计算异常期间的总用电量
                        abnormal_readings = readings_history[-continuous_alert_count:]
                        total_abnormal = sum(reading[2] for reading in abnormal_readings)
                        
                        logger.critical(f"触发用电报警: 酒店={hotel_id}, 房间={room_number}, 类型={msg_type}, 连续异常={abnormal_count}次, 总异常用电={total_abnormal}")
                        
                        # 记录异常到数据库
                        record_abnormal_usage(db, hotel_id, room_number, timestamp, total_abnormal, abnormal_count, msg_type)
                        
                        # 发送飞书通知
                        type_name = "入住后" if msg_type == MSG_TYPE_CHECKIN else "离店后"
                        message = (
                            f"酒店ID: {hotel_id}\n"
                            f"房间号: {room_number}\n"
                            f"监控类型: {type_name}\n"
                            f"连续{abnormal_count}次出现用电异常\n"
                            f"总计过量用电: {total_abnormal:.2f}度\n"
                            f"检测时间: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        send_feishu_notification(message)
                        
                        # 重置计数
                        abnormal_count = 0
                else:
                    # 重置连续计数
                    if abnormal_count > 0:
                        logger.info(f"重置异常计数: 酒店={hotel_id}, 房间={room_number}, 类型={msg_type}")
                        abnormal_count = 0
                
                # 使用线程锁保护更新操作
                with room_state_lock:
                    if room_key in room_monitoring_state:  # 确保键仍然存在
                        room_monitoring_state[room_key].update({
                            'last_reading': current_reading,
                            'last_check_time': timestamp,
                            'abnormal_count': abnormal_count,
                            'readings_history': readings_history
                        })
            else:
                logger.warning(f"未找到当前电表读数: 酒店={hotel_id}, 房间={room_number}")
    except Exception as e:
        logger.error(f"批量检查用电情况异常: {str(e)}")
    finally:
        if db:
            db.close()

def check_electricity_usage():
    """检查所有监控房间的用电情况，使用批处理方式"""
    room_count = len(room_monitoring_state)
    logger.info(f"开始检查房间用电情况, 当前监控房间数: {room_count}")
    
    if not room_monitoring_state:
        logger.info("当前没有需要监控的房间")
        return
    
    # 获取当前状态的快照（使用线程锁）
    with room_state_lock:
        # 复制键集合，因为我们可能会在迭代过程中修改字典
        room_keys = list(room_monitoring_state.keys())
    
    # 分批处理
    batch_size = CONFIG['monitoring']['batch_size']
    total_batches = (len(room_keys) + batch_size - 1) // batch_size
    
    logger.info(f"分{total_batches}批处理共{len(room_keys)}个房间, 每批{batch_size}个")
    
    # 创建处理线程池
    threads = []
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(room_keys))
        batch = room_keys[start_idx:end_idx]
        
        # 创建线程处理每批房间
        thread = Thread(target=process_room_batch, args=(batch,))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    # 等待所有批次处理完成
    for thread in threads:
        thread.join()
    
    logger.info("所有房间检查完成")

def process_message(message_data):
    """处理一条消息的具体逻辑"""
    try:
        message = json.loads(message_data)
        
        # 提取基本字段
        hotel_id = message.get('hotel_id')
        room_number = message.get('room_number')
        msg_type_code = message.get('msgType')
        
        if not all([hotel_id, room_number]):
            logger.error(f"消息格式错误，缺少必要字段hotel_id或room_number: {message_data}")
            return False
        
        # 根据msgType判断消息类型
        if msg_type_code == CONFIG['message_types']['checkin'] or (not msg_type_code and 'check_in_time' in message):
            # 处理入住消息
            check_in_time = message.get('check_in_time')
            expected_checkout_time = message.get('expected_checkout_time')
            
            if not check_in_time:
                logger.error(f"入住消息格式错误，缺少必要字段check_in_time: {message_data}")
                return False
            
            # 格式化时间字符串为datetime对象
            if isinstance(check_in_time, str):
                check_in_time = datetime.strptime(check_in_time, "%Y-%m-%d %H:%M:%S")
            
            if expected_checkout_time and isinstance(expected_checkout_time, str):
                expected_checkout_time = datetime.strptime(expected_checkout_time, "%Y-%m-%d %H:%M:%S")
            
            # 初始化入住房间监控
            success = initialize_room_monitoring(
                hotel_id, room_number, check_in_time, expected_checkout_time, MSG_TYPE_CHECKIN
            )
            
            if success:
                logger.info(f"成功初始化入住房间监控: 酒店={hotel_id}, 房间={room_number}")
            else:
                logger.warning(f"初始化入住房间监控失败: 酒店={hotel_id}, 房间={room_number}")
            
            return success
        
        elif msg_type_code == CONFIG['message_types']['checkout'] or (not msg_type_code and 'actual_checkin_time' in message):
            # 处理离店消息
            actual_checkin_time = message.get('actual_checkin_time')
            actual_checkout_time = message.get('actual_checkout_time')
            
            if not all([actual_checkin_time, actual_checkout_time]):
                logger.error(f"离店消息格式错误，缺少必要字段actual_checkin_time或actual_checkout_time: {message_data}")
                return False
            
            # 格式化时间字符串为datetime对象
            if isinstance(actual_checkin_time, str):
                actual_checkin_time = datetime.strptime(actual_checkin_time, "%Y-%m-%d %H:%M:%S")
            
            if isinstance(actual_checkout_time, str):
                actual_checkout_time = datetime.strptime(actual_checkout_time, "%Y-%m-%d %H:%M:%S")
            
            # 初始化离店房间监控
            success = initialize_room_monitoring(
                hotel_id, room_number, actual_checkin_time, actual_checkout_time, MSG_TYPE_CHECKOUT
            )
            
            if success:
                logger.info(f"成功初始化离店房间监控: 酒店={hotel_id}, 房间={room_number}")
            else:
                logger.warning(f"初始化离店房间监控失败: 酒店={hotel_id}, 房间={room_number}")
            
            return success
        
        else:
            logger.error(f"无法识别的消息类型(msgType={msg_type_code}): {message_data}")
            return False
            
    except Exception as e:
        logger.error(f"处理消息异常: {str(e)}")
        return False

def consume_messages(consumer_id):
    """消费RabbitMQ消息的函数，每个消费者有唯一ID"""
    logger.info(f"消费者 {consumer_id} 启动")
    
    while not stop_event.is_set():
        try:
            connection = connect_rabbitmq()
            if not connection:
                logger.error(f"消费者 {consumer_id} 无法连接到RabbitMQ，30秒后重试")
                time.sleep(30)
                continue
                
            channel = connection.channel()
            channel.queue_declare(queue=CONFIG['rabbitmq']['queue'], durable=True)
            
            def callback(ch, method, properties, body):
                try:
                    message_data = body.decode('utf-8')
                    logger.info(f"消费者 {consumer_id} 收到新消息: {message_data}")
                    
                    # 处理消息
                    success = process_message(message_data)
                    
                    # 确认消息
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                    if success:
                        logger.info(f"消费者 {consumer_id} 成功处理消息")
                    else:
                        logger.warning(f"消费者 {consumer_id} 处理消息失败")
                except Exception as e:
                    logger.error(f"消费者 {consumer_id} 处理消息异常: {str(e)}")
                    # 出错时也确认消息，避免反复消费导致更多错误
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # 设置预取消息数量
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=CONFIG['rabbitmq']['queue'],
                on_message_callback=callback
            )
            
            logger.info(f"消费者 {consumer_id} 开始消费RabbitMQ消息...")
            
            # 开始消费，但监听停止事件
            while not stop_event.is_set():
                # 设置短超时，定期检查停止事件
                connection.process_data_events(time_limit=1)
            
            # 收到停止信号，关闭连接
            channel.close()
            connection.close()
            logger.info(f"消费者 {consumer_id} 收到停止信号，已关闭连接")
            return
            
        except Exception as e:
            logger.error(f"消费者 {consumer_id} 消费消息过程发生异常: {str(e)}")
            logger.info(f"消费者 {consumer_id} 30秒后尝试重新连接...")
            
            # 检查是否收到停止信号
            if stop_event.wait(timeout=30):
                logger.info(f"消费者 {consumer_id} 收到停止信号，终止重连")
                return
        finally:
            if connection and not connection.is_closed:
                connection.close()

def shutdown_gracefully():
    """优雅关闭系统"""
    logger.info("开始优雅关闭系统...")
    
    # 设置停止事件，通知所有线程
    stop_event.set()
    
    # 等待线程结束
    logger.info("等待所有线程结束...")
    time.sleep(5)  # 给线程一些时间来清理
    
    logger.info("系统已关闭")

def main():
    """主函数"""
    try:
        logger.info("启动酒店用电异常监控系统...")
        
        # 初始化MySQL连接池
        global mysql_pool
        mysql_pool = create_mysql_pool()
        if not mysql_pool:
            logger.error("无法创建MySQL连接池，系统无法启动")
            return
        
        # 启动多个消费者线程
        consumer_threads = []
        consumer_count = CONFIG['performance']['consumer_count']
        logger.info(f"启动 {consumer_count} 个消费者线程")
        
        for i in range(consumer_count):
            consumer_thread = Thread(target=consume_messages, args=(i+1,))
            consumer_thread.daemon = True
            consumer_thread.start()
            consumer_threads.append(consumer_thread)
            logger.info(f"消费者线程 {i+1} 已启动")
        
        # 设置定时任务，按配置的间隔检查用电情况
        check_interval = CONFIG['monitoring']['check_interval_minutes']
        schedule.every(check_interval).minutes.do(check_electricity_usage)
        logger.info(f"已设置定时检查任务，间隔={check_interval}分钟")
        
        # 添加系统状态打印任务
        schedule.every(10).minutes.do(lambda: logger.info(f"系统运行中，当前监控房间数: {len(room_monitoring_state)}"))
        
        # 运行定时任务
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到终止信号，正在优雅关闭...")
        shutdown_gracefully()
    except Exception as e:
        logger.error(f"主程序异常: {str(e)}")
        shutdown_gracefully()
    finally:
        logger.info("酒店用电异常监控系统已停止")

if __name__ == "__main__":
    main() 