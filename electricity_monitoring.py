# -*- coding: utf-8 -*-

import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from threading import Thread, Lock, Event

import mysql.connector
import pika
import redis
import requests
import schedule
from mysql.connector import pooling
from snowflake import SnowflakeGenerator

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
        'host': '192.168.1.204',
        'port': 5672,
        'virtual_host': '/',
        'username': 'zxtf',
        'password': 'zxtf123',
        'queue': 'MQ.PMS.CHECKIN.CHECKOUT.TEST'
    },
    'mysql': {
        'host': '192.168.1.204',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'zxtf_saas_test',
        'pool_name': 'hotel_monitoring_pool',
        'pool_size': 10,  # 连接池大小
        'ssl_disabled': True  # 禁用SSL连接
    },
    'redis': {
        'host': '192.168.1.204',  # Redis服务器地址，需要根据实际情况修改
        'port': 6379,            # Redis端口
        'db': 0,                 # 使用的数据库编号
        'password': 123456,        # Redis密码，如果有的话
        'key_prefix': 'hotel_monitoring:', # Redis键前缀
        'expire_time': 86400 * 7 # 键过期时间，默认7天
    },
    'feishu': {
        'webhook_url': 'https://open.feishu.cn/open-apis/bot/v2/hook/1f35ca70-1c95-43f4-a253-6d4eed59818e'
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

# 全局连接池
mysql_pool = None
redis_conn = None

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
            'database': CONFIG['mysql']['database'],
            'ssl_disabled': True  # 禁用SSL连接
        }
        
        pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
        logger.info(f"MySQL连接池创建成功，池大小={CONFIG['mysql']['pool_size']}")
        return pool
    except Exception as e:
        logger.error(f"创建MySQL连接池失败: {str(e)}")
        return None

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

def find_closest_electricity_reading(db, room_number, reference_time, product_keys=None):
    """查找与参考时间最接近的电表读数
    
    Args:
        db: 数据库连接
        room_number: 房间号
        reference_time: 参考时间
        product_keys: 产品Key列表，用于筛选同一酒店的设备
    
    Returns:
        最接近的电表读数或None
    """
    try:
        cursor = db.cursor(dictionary=True)
        
        if product_keys and len(product_keys) > 0:
            # 构建IN查询的参数占位符
            placeholders = ', '.join(['%s'] * len(product_keys))
            
            query = f"""
            SELECT id, value as reading_value, gmt_modified as script_time, 
                   device_name, power, product_key
            FROM smart_meter 
            WHERE room_num = %s 
              AND product_key IN ({placeholders})
            ORDER BY ABS(TIMESTAMPDIFF(SECOND, gmt_modified, %s)) 
            LIMIT 1
            """
            
            # 构建查询参数
            params = [room_number] + product_keys + [reference_time]
            cursor.execute(query, params)
        else:
            # 如果没有提供product_key，则只按房间号查询
            query = """
            SELECT id, value as reading_value, gmt_modified as script_time, 
                   device_name, power, product_key
            FROM smart_meter 
            WHERE room_num = %s 
            ORDER BY ABS(TIMESTAMPDIFF(SECOND, gmt_modified, %s)) 
            LIMIT 1
            """
            cursor.execute(query, (room_number, reference_time))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            logger.debug(f"找到最接近时间的电表读数: 房间={room_number}, 产品Key={result.get('product_key', 'unknown')}")
        
        return result
    except Exception as e:
        logger.error(f"查询最近电表读数失败: {str(e)}")
        return None

def initialize_room_monitoring(dept_code, room_number, reference_time, end_time, msg_type):
    """初始化房间监控状态
    
    Args:
        dept_code: 部门编码(门店编码)
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
        
        # 根据dept_code获取product_keys
        product_keys = get_product_key_by_dept_code(db, dept_code)
        if not product_keys:
            logger.warning(f"未找到部门对应的产品Key, 将只按房间号查询: dept_code={dept_code}, 房间={room_number}")
        else:
            logger.info(f"找到部门对应的产品Key: dept_code={dept_code}, product_keys={product_keys}")
            
        # 查询最接近参考时间的用电量记录
        result = find_closest_electricity_reading(db, room_number, reference_time, product_keys)
        
        if result:
            initial_reading = float(result['reading_value'])
            timestamp = result['script_time']
            device_name = result.get('device_name', '未知设备')
            product_key = result.get('product_key', '')
            
            # 生成房间唯一键，不再使用hotel_id，仅使用房间号和消息类型区分不同监控
            room_key = f"{room_number}_{msg_type}"
            
            # 获取适用的阈值
            if msg_type == MSG_TYPE_CHECKIN:
                threshold = CONFIG['monitoring']['checkin_abnormal_threshold']
            else:  # MSG_TYPE_CHECKOUT
                threshold = CONFIG['monitoring']['checkout_abnormal_threshold']
            
            # 创建状态数据
            state_data = {
                'dept_code': dept_code,
                'room_number': room_number,
                'product_key': product_key,
                'device_name': device_name,
                'reference_time': reference_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(reference_time, datetime) else reference_time,
                'end_time': end_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(end_time, datetime) else end_time,
                'initial_reading': initial_reading,
                'last_reading': initial_reading,
                'last_check_time': timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else timestamp,
                'abnormal_count': 0,
                'readings_history': [],
                'msg_type': msg_type,
                'threshold': threshold
            }
            
            # 保存到Redis
            if not save_state_to_redis(room_key, state_data):
                logger.error(f"保存房间状态到Redis失败: 房间={room_number}")
                # 如果Redis保存失败，回退到内存存储
                with room_state_lock:
                    room_monitoring_state[room_key] = state_data
                    logger.warning(f"已回退到内存存储房间状态: 房间={room_number}")
            
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
            (dept_code, room_number, {time_field_name}, {end_time_field_name}, 
            initial_reading, monitoring_start_time, msg_type, product_key, device_name) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                dept_code, 
                room_number, 
                reference_time, 
                end_time, 
                initial_reading, 
                timestamp,
                msg_type,
                product_key,
                device_name
            ))
            db.commit()
            cursor.close()
            
            logger.info(f"已初始化房间{msg_type}监控: 部门编码={dept_code}, 房间={room_number}, 初始读数={initial_reading}, 阈值={threshold}")
            return True
        else:
            logger.warning(f"未找到房间电表读数: 部门编码={dept_code}, 房间={room_number}")
            return False
    except Exception as e:
        logger.error(f"初始化房间监控异常: {str(e)}")
        return False
    finally:
        if db:
            db.close()

def get_current_electricity_reading(db, room_number, product_keys=None):
    """获取当前电表读数
    
    Args:
        db: 数据库连接
        room_number: 房间号
        product_keys: 产品Key列表，用于筛选同一酒店的设备
    
    Returns:
        当前电表读数或None
    """
    try:
        cursor = db.cursor(dictionary=True)
        
        if product_keys and len(product_keys) > 0:
            # 构建IN查询的参数占位符
            placeholders = ', '.join(['%s'] * len(product_keys))
            
            query = f"""
            SELECT value as reading_value, gmt_modified as script_time, power, 
                   device_name, product_key, device_key, iot_id
            FROM smart_meter 
            WHERE room_num = %s 
              AND product_key IN ({placeholders})
            ORDER BY gmt_modified DESC 
            LIMIT 1
            """
            
            # 构建查询参数
            params = [room_number] + product_keys
            cursor.execute(query, params)
        else:
            # 如果没有提供product_key，则只按房间号查询
            query = """
            SELECT value as reading_value, gmt_modified as script_time, power,
                   device_name, product_key, device_key, iot_id
            FROM smart_meter 
            WHERE room_num = %s 
            ORDER BY gmt_modified DESC 
            LIMIT 1
            """
            cursor.execute(query, (room_number,))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            logger.debug(f"获取到当前电表读数: 房间={room_number}, 产品Key={result.get('product_key', 'unknown')}")
        
        return result
    except Exception as e:
        logger.error(f"获取当前电表读数失败: {str(e)}")
        return None

def record_abnormal_usage(db, dept_code, room_number, detected_time, total_consumption, continuous_periods, msg_type, device_name=None, current_power=0):
    """记录异常用电情况到数据库
    
    Args:
        db: 数据库连接
        dept_code: 部门编码(门店编码)
        room_number: 房间号
        detected_time: 检测时间
        total_consumption: 总消耗电量
        continuous_periods: 连续异常周期数
        msg_type: 消息类型
        device_name: 设备名称，如果为None则从数据库获取
        current_power: 当前功率
    """
    try:
        cursor = db.cursor()
        
        # 使用snowflake算法生成主键ID
        snowflake_gen = SnowflakeGenerator(1, 1)  # datacenter_id=1, worker_id=1
        unique_id = str(next(snowflake_gen))
        
        # 如果未提供设备名称，则从数据库获取
        if device_name is None:
            # 尝试获取产品Key
            product_keys = get_product_key_by_dept_code(db, dept_code)
            
            device_info_cursor = db.cursor(dictionary=True)
            if product_keys:
                # 构建IN查询的参数占位符
                placeholders = ', '.join(['%s'] * len(product_keys))
                device_query = f"""
                SELECT device_name, power 
                FROM smart_meter 
                WHERE room_num = %s 
                  AND product_key IN ({placeholders})
                ORDER BY gmt_modified DESC 
                LIMIT 1
                """
                params = [room_number] + product_keys
                device_info_cursor.execute(device_query, params)
            else:
                device_query = """
                SELECT device_name, power 
                FROM smart_meter 
                WHERE room_num = %s 
                ORDER BY gmt_modified DESC 
                LIMIT 1
                """
                device_info_cursor.execute(device_query, (room_number,))
            
            device_info = device_info_cursor.fetchone()
            device_info_cursor.close()
            
            if device_info and 'device_name' in device_info:
                device_name = device_info['device_name']
                if current_power == 0 and 'power' in device_info:
                    current_power = float(device_info['power'])
            else:
                device_name = "未知设备"
        
        # 获取部门信息
        dept_info_cursor = db.cursor(dictionary=True)
        dept_query = """
        SELECT dept_id, dept_code, dept_name, dept_brand 
        FROM iot_product_dept 
        WHERE dept_code = %s 
        LIMIT 1
        """
        dept_info_cursor.execute(dept_query, (dept_code,))
        dept_info = dept_info_cursor.fetchone()
        dept_info_cursor.close()
        
        dept_name = dept_info['dept_name'] if dept_info and 'dept_name' in dept_info else "未知酒店"
        dept_id = dept_info['dept_id'] if dept_info and 'dept_id' in dept_info else dept_code
        dept_brand = dept_info['dept_brand'] if dept_info and 'dept_brand' in dept_info else "未知品牌"
        
        # 确定房态和入住/退房时间
        room_status = "已入住" if msg_type == MSG_TYPE_CHECKIN else "已退房"
        
        # 计算电费（假设每度电1元，可以根据实际情况调整）
        electricity_cost = float(total_consumption) * 1.0
        
        # 确定用电状态
        electricity_usage_status = "异常"
        
        # 插入新表
        insert_query = """
        INSERT INTO abnormal_electricity_usage 
        (id, dept_name, dept_id, dept_brand, room_num, device_name, room_status, 
        check_in_time, check_out_time, electricity_usage_status, abnormal_time, 
        power_consumption, estimated_electricity_cost) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 获取房间状态相关时间
        room_state_key = f"{room_number}_{msg_type}"
        with room_state_lock:
            if room_state_key in room_monitoring_state:
                state = room_monitoring_state[room_state_key]
                reference_time = state.get('reference_time')
                end_time = state.get('end_time')
            else:
                state = load_state_from_redis(room_state_key)
                if state:
                    reference_time = state.get('reference_time')
                    end_time = state.get('end_time')
                else:
                    reference_time = None
                    end_time = None
        
        # 根据消息类型设置入住和退房时间
        if msg_type == MSG_TYPE_CHECKIN:
            check_in_time = reference_time
            check_out_time = end_time
        else:  # MSG_TYPE_CHECKOUT
            check_in_time = None  # 这里可能需要从历史记录中获取
            check_out_time = reference_time
        
        # 处理字符串格式的时间
        if isinstance(check_in_time, str):
            check_in_time = datetime.strptime(check_in_time, "%Y-%m-%d %H:%M:%S")
        if isinstance(check_out_time, str):
            check_out_time = datetime.strptime(check_out_time, "%Y-%m-%d %H:%M:%S")
        
        cursor.execute(insert_query, (
            unique_id,
            dept_name,
            dept_id,
            dept_brand,
            room_number,
            device_name,
            room_status,
            check_in_time,
            check_out_time,
            electricity_usage_status,
            detected_time,
            total_consumption,
            electricity_cost
        ))
        
        # 同时保持对原表的支持，以确保系统兼容性
        original_insert_query = """
        INSERT INTO electricity_abnormal 
        (dept_code, room_number, detected_time, total_consumption, continuous_periods, is_notified, msg_type) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(original_insert_query, (
            dept_code, 
            room_number, 
            detected_time, 
            total_consumption, 
            continuous_periods,
            True,
            msg_type
        ))
        
        db.commit()
        cursor.close()
        logger.info(f"已记录异常用电情况到新表: 部门编码={dept_code}, 房间={room_number}, 设备={device_name}, 电量={total_consumption}, 功率={current_power}W, 时间={detected_time}")
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
            # 首先尝试从Redis获取状态数据
            state = load_state_from_redis(room_key)
            
            # 如果Redis中不存在，则尝试从内存中获取
            if state is None:
                with room_state_lock:
                    if room_key not in room_monitoring_state:
                        logger.debug(f"找不到房间监控状态: key={room_key}")
                        continue
                    state = room_monitoring_state[room_key].copy()  # 复制一份避免竞态条件
            
            dept_code = state.get('dept_code')
            room_number = state['room_number']
            product_key = state.get('product_key')
            msg_type = state.get('msg_type', MSG_TYPE_CHECKIN)  # 默认为入住类型，兼容旧数据
            threshold = state.get('threshold', CONFIG['monitoring']['checkin_abnormal_threshold'])  # 获取阈值
            
            # 检查是否已经超过结束时间
            if 'end_time' in state and state['end_time']:
                end_time = state['end_time']
                if isinstance(end_time, str):
                    end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
                
                if datetime.now() > end_time:
                    logger.info(f"房间已超过监控结束时间，停止监控: 部门编码={dept_code}, 房间={room_number}, 类型={msg_type}")
                    # 从Redis中删除数据
                    delete_state_from_redis(room_key)
                    # 也从内存中删除（如果存在）
                    with room_state_lock:
                        if room_key in room_monitoring_state:
                            del room_monitoring_state[room_key]
                    continue
            
            # 如果有product_key，则使用该信息查询电表数据
            product_keys = None
            if product_key:
                product_keys = [product_key]
            elif dept_code:
                # 如果没有product_key但有dept_code，则重新获取产品key
                product_keys = get_product_key_by_dept_code(db, dept_code)
            
            # 查询当前用电量
            result = get_current_electricity_reading(db, room_number, product_keys)
            
            if result:
                current_reading = float(result['reading_value'])
                timestamp = result['script_time']
                current_power = float(result.get('power', 0))  # 获取当前功率
                device_name = result.get('device_name', state.get('device_name', '未知设备'))
                product_key_from_db = result.get('product_key', '')
                
                # 解析last_check_time（如果是字符串）
                last_check_time = state['last_check_time']
                if isinstance(last_check_time, str):
                    last_check_time = datetime.strptime(last_check_time, "%Y-%m-%d %H:%M:%S")
                
                # 计算差值
                last_reading = float(state['last_reading'])
                diff = current_reading - last_reading
                
                logger.info(f"房间用电检查: 部门编码={dept_code}, 房间={room_number}, 设备={device_name}, 类型={msg_type}, 当前读数={current_reading}, 当前功率={current_power}W, 上次读数={last_reading}, 差值={diff}, 阈值={threshold}")
                
                # 更新状态（添加新读数和差值）
                readings_history = state['readings_history'].copy()
                
                # 确保timestamp是字符串格式，以便JSON序列化
                timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else timestamp
                
                readings_history.append((timestamp_str, current_reading, diff, current_power))
                
                # 限制历史记录数量
                max_history_size = CONFIG['monitoring']['max_history_size']
                if len(readings_history) > max_history_size:
                    readings_history = readings_history[-max_history_size:]
                
                # 检查是否超过阈值
                abnormal_count = state['abnormal_count']
                if diff > threshold:
                    abnormal_count += 1
                    logger.warning(f"检测到异常用电: 部门编码={dept_code}, 房间={room_number}, 类型={msg_type}, 差值={diff}, 连续次数={abnormal_count}")
                    
                    # 如果连续异常次数达到阈值，触发报警
                    continuous_alert_count = CONFIG['monitoring']['continuous_alert_count']
                    if abnormal_count >= continuous_alert_count:
                        # 计算异常期间的总用电量
                        abnormal_readings = readings_history[-continuous_alert_count:]
                        total_abnormal = sum(reading[2] for reading in abnormal_readings)
                        
                        logger.critical(f"触发用电报警: 部门编码={dept_code}, 房间={room_number}, 类型={msg_type}, 连续异常={abnormal_count}次, 总异常用电={total_abnormal}")
                        
                        # 使用数据库中查到的设备信息
                        
                        # 记录异常到数据库
                        record_abnormal_usage(db, dept_code, room_number, timestamp, total_abnormal, abnormal_count, msg_type, device_name=device_name, current_power=current_power)
                        
                        # 发送飞书通知
                        type_name = "入住后" if msg_type == MSG_TYPE_CHECKIN else "离店后"
                        
                        message = (
                            f"部门编码: {dept_code}\n"
                            f"房间号: {room_number}\n"
                            f"设备名称: {device_name}\n"
                            f"产品Key: {product_key_from_db}\n"
                            f"监控类型: {type_name}\n"
                            f"连续{abnormal_count}次出现用电异常\n"
                            f"总计过量用电: {total_abnormal:.2f}度\n"
                            f"当前功率: {current_power}W\n"
                            f"预估电费: {total_abnormal * 1.0:.2f}元\n"
                            f"检测时间: {timestamp_str if isinstance(timestamp, str) else timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        send_feishu_notification(message)
                        
                        # 重置计数
                        abnormal_count = 0
                else:
                    # 重置连续计数
                    if abnormal_count > 0:
                        logger.info(f"重置异常计数: 部门编码={dept_code}, 房间={room_number}, 类型={msg_type}")
                        abnormal_count = 0
                
                # 创建更新后的状态
                updated_state = state.copy()
                updated_state.update({
                    'last_reading': current_reading,
                    'last_check_time': timestamp_str if isinstance(timestamp, str) else timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    'abnormal_count': abnormal_count,
                    'readings_history': readings_history,
                    'device_name': device_name,
                    'product_key': product_key_from_db or product_key  # 优先使用新的产品key
                })
                
                # 保存到Redis
                if not save_state_to_redis(room_key, updated_state):
                    logger.error(f"更新房间状态到Redis失败: 房间={room_number}")
                    # 如果Redis保存失败，回退到内存存储
                    with room_state_lock:
                        if room_key in room_monitoring_state:  # 确保键仍然存在
                            room_monitoring_state[room_key].update({
                                'last_reading': current_reading,
                                'last_check_time': timestamp,
                                'abnormal_count': abnormal_count,
                                'readings_history': readings_history,
                                'device_name': device_name,
                                'product_key': product_key_from_db or product_key
                            })
                            logger.warning(f"已回退到内存更新房间状态: 房间={room_number}")
            else:
                logger.warning(f"未找到当前电表读数: 部门编码={dept_code}, 房间={room_number}")
    except Exception as e:
        logger.error(f"批量检查用电情况异常: {str(e)}")
    finally:
        if db:
            db.close()

def check_electricity_usage():
    """检查所有监控房间的用电情况，使用批处理方式"""
    # 首先尝试从Redis获取所有监控房间的键
    redis_room_keys = get_all_monitoring_keys()
    
    # 如果Redis中有键，使用Redis的键
    if redis_room_keys:
        room_keys = redis_room_keys
        room_count = len(room_keys)
        logger.info(f"从Redis获取监控房间, 数量: {room_count}")
    else:
        # 否则，使用内存中的状态数据
        with room_state_lock:
            room_count = len(room_monitoring_state)
            room_keys = list(room_monitoring_state.keys())
        logger.info(f"从内存获取监控房间, 数量: {room_count}")
    
    logger.info(f"开始检查房间用电情况, 当前监控房间数: {room_count}")
    
    if not room_keys:
        logger.info("当前没有需要监控的房间")
        return
    
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
        dept_code = message.get('dept_code')  # 使用dept_code替代hotel_id
        room_number = message.get('room_number')
        msg_type_code = message.get('msgType')
        
        if not all([dept_code, room_number]):
            logger.error(f"消息格式错误，缺少必要字段dept_code或room_number: {message_data}")
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
                dept_code, room_number, check_in_time, expected_checkout_time, MSG_TYPE_CHECKIN
            )
            
            if success:
                logger.info(f"成功初始化入住房间监控: 部门编码={dept_code}, 房间={room_number}")
            else:
                logger.warning(f"初始化入住房间监控失败: 部门编码={dept_code}, 房间={room_number}")
            
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
                dept_code, room_number, actual_checkin_time, actual_checkout_time, MSG_TYPE_CHECKOUT
            )
            
            if success:
                logger.info(f"成功初始化离店房间监控: 部门编码={dept_code}, 房间={room_number}")
            else:
                logger.warning(f"初始化离店房间监控失败: 部门编码={dept_code}, 房间={room_number}")
            
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
    
    # 关闭Redis连接
    global redis_conn
    if redis_conn:
        try:
            redis_conn.close()
            logger.info("已关闭Redis连接")
        except:
            pass
    
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
        
        # 初始化Redis连接
        global redis_conn
        redis_conn = create_redis_connection()
        if not redis_conn:
            logger.warning("无法创建Redis连接，将使用内存存储状态数据")
        else:
            logger.info("Redis连接已建立，将使用Redis存储状态数据")
            
            # 尝试加载已有的房间状态数据
            redis_room_keys = get_all_monitoring_keys()
            if redis_room_keys:
                logger.info(f"从Redis加载已有监控状态，共{len(redis_room_keys)}个房间")
                for room_key in redis_room_keys:
                    state = load_state_from_redis(room_key)
                    if state:
                        with room_state_lock:
                            room_monitoring_state[room_key] = state
                logger.info(f"已将Redis中的房间状态加载到内存，共{len(room_monitoring_state)}个房间")
        
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
        def print_system_status():
            # 首先尝试从Redis获取所有监控房间的键
            redis_room_keys = get_all_monitoring_keys()
            redis_count = len(redis_room_keys)
            
            # 从内存中获取房间数
            memory_count = len(room_monitoring_state)
            
            if redis_conn:
                logger.info(f"系统运行中，Redis监控房间数: {redis_count}, 内存监控房间数: {memory_count}")
            else:
                logger.info(f"系统运行中，当前监控房间数: {memory_count}")
            
            return True
        
        schedule.every(10).minutes.do(print_system_status)
        
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

def create_redis_connection():
    """创建Redis连接"""
    try:
        conn = redis.Redis(
            host=CONFIG['redis']['host'],
            port=CONFIG['redis']['port'],
            db=CONFIG['redis']['db'],
            password=CONFIG['redis']['password'],
            decode_responses=True  # 自动将字节解码为字符串
        )
        # 测试连接
        conn.ping()
        logger.info(f"Redis连接创建成功，地址={CONFIG['redis']['host']}:{CONFIG['redis']['port']}")
        return conn
    except Exception as e:
        logger.error(f"创建Redis连接失败: {str(e)}")
        return None

def get_redis_connection():
    """获取Redis连接"""
    global redis_conn
    
    if redis_conn is None:
        redis_conn = create_redis_connection()
    
    # 如果连接断开，重新连接
    if redis_conn is not None:
        try:
            redis_conn.ping()
        except:
            logger.warning("Redis连接已断开，尝试重新连接")
            redis_conn = create_redis_connection()
    
    return redis_conn

def save_state_to_redis(room_key, state_data):
    """将房间监控状态保存到Redis"""
    try:
        conn = get_redis_connection()
        if conn is None:
            logger.error(f"保存状态到Redis失败: 无法获取Redis连接")
            return False
            
        # 序列化状态数据
        serialized_data = json.dumps(state_data)
        
        # 生成Redis键
        redis_key = f"{CONFIG['redis']['key_prefix']}{room_key}"
        
        # 保存到Redis并设置过期时间
        conn.set(redis_key, serialized_data)
        conn.expire(redis_key, CONFIG['redis']['expire_time'])
        
        logger.debug(f"成功保存状态到Redis: key={redis_key}")
        return True
    except Exception as e:
        logger.error(f"保存状态到Redis异常: {str(e)}")
        return False

def load_state_from_redis(room_key):
    """从Redis加载房间监控状态"""
    try:
        conn = get_redis_connection()
        if conn is None:
            logger.error(f"从Redis加载状态失败: 无法获取Redis连接")
            return None
            
        # 生成Redis键
        redis_key = f"{CONFIG['redis']['key_prefix']}{room_key}"
        
        # 从Redis获取数据
        data = conn.get(redis_key)
        
        if data:
            # 反序列化数据
            state_data = json.loads(data)
            logger.debug(f"成功从Redis加载状态: key={redis_key}")
            return state_data
        else:
            logger.debug(f"Redis中不存在此键: key={redis_key}")
            return None
    except Exception as e:
        logger.error(f"从Redis加载状态异常: {str(e)}")
        return None

def delete_state_from_redis(room_key):
    """从Redis删除房间监控状态"""
    try:
        conn = get_redis_connection()
        if conn is None:
            logger.error(f"从Redis删除状态失败: 无法获取Redis连接")
            return False
            
        # 生成Redis键
        redis_key = f"{CONFIG['redis']['key_prefix']}{room_key}"
        
        # 从Redis删除数据
        conn.delete(redis_key)
        logger.debug(f"成功从Redis删除状态: key={redis_key}")
        return True
    except Exception as e:
        logger.error(f"从Redis删除状态异常: {str(e)}")
        return False

def get_all_monitoring_keys():
    """获取所有被监控的房间键"""
    try:
        conn = get_redis_connection()
        if conn is None:
            logger.error("获取监控房间键失败: 无法获取Redis连接")
            return []
            
        # 使用模式匹配查找所有键
        pattern = f"{CONFIG['redis']['key_prefix']}*"
        all_keys = conn.keys(pattern)
        
        # 移除前缀
        prefix_len = len(CONFIG['redis']['key_prefix'])
        room_keys = [key[prefix_len:] for key in all_keys]
        
        return room_keys
    except Exception as e:
        logger.error(f"获取监控房间键异常: {str(e)}")
        return []

def get_product_key_by_dept_code(db, dept_code):
    """根据部门编码(dept_code)查询对应的product_key
    
    Args:
        db: 数据库连接
        dept_code: 部门编码
    
    Returns:
        product_key列表或None
    """
    try:
        cursor = db.cursor(dictionary=True)
        query = """
        SELECT product_key 
        FROM iot_product_dept 
        WHERE dept_code = %s
        """
        cursor.execute(query, (dept_code,))
        results = cursor.fetchall()
        cursor.close()
        
        if results:
            # 返回所有找到的product_key列表
            return [result['product_key'] for result in results]
        else:
            logger.warning(f"未找到部门编码对应的产品Key: dept_code={dept_code}")
            return None
    except Exception as e:
        logger.error(f"查询部门对应产品Key失败: {str(e)}")
        return None

if __name__ == "__main__":
    main() 