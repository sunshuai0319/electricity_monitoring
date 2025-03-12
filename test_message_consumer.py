# -*- coding: utf-8 -*-

import pika
import json
import time
import signal
import sys
from datetime import datetime

# 全局变量，用于停止消费
stop_consuming = False

def signal_handler(sig, frame):
    """处理终止信号"""
    global stop_consuming
    print("\n收到终止信号，正在关闭消费者...")
    stop_consuming = True

# 注册信号处理函数
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def connect_rabbitmq():
    """连接到RabbitMQ并返回连接和通道"""
    try:
        # 使用与test_message_sender.py相同的连接参数
        credentials = pika.PlainCredentials('zxtf', 'zxtf123')
        parameters = pika.ConnectionParameters(
            host='192.168.1.204',
            port=5672,
            virtual_host='/',
            credentials=credentials,
            heartbeat=600  # 心跳超时设置为10分钟
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # 声明队列（确保队列存在）
        queue_name = 'hotel_checkin_queue'
        channel.queue_declare(queue=queue_name, durable=True)
        
        print(f"已连接到RabbitMQ服务器 192.168.1.204:5672")
        print(f"正在监听队列: {queue_name}")
        
        return connection, channel, queue_name
    except Exception as e:
        print(f"连接RabbitMQ失败: {str(e)}")
        return None, None, None

def callback(ch, method, properties, body):
    """处理接收到的消息"""
    try:
        # 解码消息内容
        message_data = json.loads(body.decode('utf-8'))
        
        # 确定消息类型
        msg_type = message_data.get('msgType', '未知')
        if msg_type == '5710005':
            msg_type_desc = "入住消息"
        elif msg_type == '5710013':
            msg_type_desc = "离店消息"
        else:
            msg_type_desc = f"未知消息类型({msg_type})"
        
        # 打印接收时间和消息内容
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{current_time}] 收到{msg_type_desc}:")
        print(json.dumps(message_data, indent=2, ensure_ascii=False))
        
        # 检查消息格式是否符合要求
        required_fields = ['dept_code', 'room_number']
        missing_fields = [field for field in required_fields if field not in message_data]
        
        if missing_fields:
            print(f"警告: 消息缺少必要字段: {', '.join(missing_fields)}")
        else:
            print(f"消息格式验证通过")
            
            # 验证消息类型特有字段
            if msg_type == '5710005':  # 入住消息
                if 'check_in_time' not in message_data or 'expected_checkout_time' not in message_data:
                    print("警告: 入住消息缺少check_in_time或expected_checkout_time字段")
            elif msg_type == '5710013':  # 离店消息
                if 'actual_checkin_time' not in message_data or 'actual_checkout_time' not in message_data:
                    print("警告: 离店消息缺少actual_checkin_time或actual_checkout_time字段")
        
        # 确认消息
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"处理消息失败: {str(e)}")
        # 出错时也确认消息，避免反复消费
        ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consuming():
    """开始消费消息"""
    connection, channel, queue_name = connect_rabbitmq()
    
    if not connection or not channel:
        print("无法启动消费者，因为连接失败")
        return False
    
    try:
        # 设置每次只获取一条消息
        channel.basic_qos(prefetch_count=1)
        
        # 注册消费者回调函数
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback
        )
        
        print("消费者已启动，等待消息...(按 CTRL+C 退出)")
        print("-" * 50)
        
        # 开始消费，但监听停止标志
        while not stop_consuming:
            try:
                # 设置短超时，定期检查停止标志
                connection.process_data_events(time_limit=1)
            except pika.exceptions.AMQPError as e:
                print(f"AMQP错误: {str(e)}")
                print("尝试重新连接...")
                
                # 尝试重新连接
                connection.close()
                connection, channel, queue_name = connect_rabbitmq()
                
                if not connection or not channel:
                    print("重新连接失败，消费者将退出")
                    return False
                
                # 重新注册消费者
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=callback
                )
                
                print("已重新连接，继续监听...")
        
        # 收到停止信号，关闭连接
        if connection and connection.is_open:
            channel.close()
            connection.close()
            print("已关闭RabbitMQ连接")
        
        return True
    except KeyboardInterrupt:
        print("\n接收到退出信号，正在关闭...")
        if connection and connection.is_open:
            channel.close()
            connection.close()
            print("已关闭RabbitMQ连接")
        return True
    except Exception as e:
        print(f"消费过程出错: {str(e)}")
        if connection and connection.is_open:
            connection.close()
        return False

def main():
    """主函数"""
    print("=== RabbitMQ消息测试消费者 ===")
    
    try:
        success = start_consuming()
        if success:
            print("消费者正常退出")
        else:
            print("消费者异常退出")
    except Exception as e:
        print(f"程序异常: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 