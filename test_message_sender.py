# -*- coding: utf-8 -*-

import pika
import json
import argparse
from datetime import datetime, timedelta

# 消息类型常量
MSG_TYPE_CHECKIN = '5710005'   # 入住消息类型
MSG_TYPE_CHECKOUT = '5710013'  # 离店消息类型

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='发送测试入住/离店消息到RabbitMQ')
    
    parser.add_argument('--host', default='192.168.1.204', help='RabbitMQ服务器地址')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ服务器端口')
    parser.add_argument('--vhost', default='/', help='RabbitMQ虚拟主机')
    parser.add_argument('--username', default='zxtf', help='RabbitMQ用户名')
    parser.add_argument('--password', default='zxtf123', help='RabbitMQ密码')
    parser.add_argument('--exchange', default='EX.FANOUT.PMS.CHECKIN.CHECKOUT.TEST', help='目标交换机名称')

    parser.add_argument('--dept-code', required=True, help='部门编码(门店编码)')
    parser.add_argument('--room-number', required=True, help='房间号')
    
    # 消息类型参数
    parser.add_argument('--msg-type', choices=['checkin', 'checkout'], default='checkin', 
                       help='消息类型: checkin-入住消息, checkout-离店消息')
    
    # 入住消息参数
    parser.add_argument('--checkin-time', help='实际抵店时间，默认为当前时间')
    parser.add_argument('--expected-checkout-time', help='预计离店时间，默认为入住时间后2天')
    
    # 离店消息参数
    parser.add_argument('--actual-checkin-time', help='实际抵店时间，默认为3天前')
    parser.add_argument('--actual-checkout-time', help='实际离店时间，默认为当前时间')
    
    return parser.parse_args()

def send_test_message(args):
    """发送测试消息到RabbitMQ"""
    message = {
        'dept_code': args.dept_code,  # 使用dept_code代替hotel_id
        'room_number': args.room_number
    }
    
    # 根据消息类型设置不同字段
    if args.msg_type == 'checkin':
        # 处理入住消息时间参数
        if args.checkin_time:
            check_in_time = datetime.strptime(args.checkin_time, '%Y-%m-%d %H:%M:%S')
        else:
            check_in_time = datetime.now()
        
        if args.expected_checkout_time:
            expected_checkout_time = datetime.strptime(args.expected_checkout_time, '%Y-%m-%d %H:%M:%S')
        else:
            expected_checkout_time = check_in_time + timedelta(days=2)
        
        # 设置入住消息内容
        message.update({
            'msgType': MSG_TYPE_CHECKIN,
            'check_in_time': check_in_time.strftime('%Y-%m-%d %H:%M:%S'),
            'expected_checkout_time': expected_checkout_time.strftime('%Y-%m-%d %H:%M:%S')
        })
        message_type_desc = "入住消息"
    else:  # checkout
        # 处理离店消息时间参数
        if args.actual_checkin_time:
            actual_checkin_time = datetime.strptime(args.actual_checkin_time, '%Y-%m-%d %H:%M:%S')
        else:
            actual_checkin_time = datetime.now() - timedelta(days=3)
        
        if args.actual_checkout_time:
            actual_checkout_time = datetime.strptime(args.actual_checkout_time, '%Y-%m-%d %H:%M:%S')
        else:
            actual_checkout_time = datetime.now()
        
        # 设置离店消息内容
        message.update({
            'msgType': MSG_TYPE_CHECKOUT,
            'actual_checkin_time': actual_checkin_time.strftime('%Y-%m-%d %H:%M:%S'),
            'actual_checkout_time': actual_checkout_time.strftime('%Y-%m-%d %H:%M:%S')
        })
        message_type_desc = "离店消息"
    
    # 连接到RabbitMQ
    try:
        credentials = pika.PlainCredentials(args.username, args.password)
        parameters = pika.ConnectionParameters(
            host=args.host,
            port=args.port,
            virtual_host=args.vhost,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # 声明交换机（确保交换机存在）
        channel.exchange_declare(
            exchange=args.exchange,
            exchange_type='fanout',
            durable=True
        )
        
        # 发送消息到交换机
        channel.basic_publish(
            exchange=args.exchange,
            routing_key='',  # fanout交换机忽略routing_key
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # 持久化消息
                content_type='application/json'
            )
        )
        
        print(f"{message_type_desc}已发送到交换机 {args.exchange}:")
        print(json.dumps(message, indent=2, ensure_ascii=False))
        
        connection.close()
        return True
    except Exception as e:
        print(f"发送消息时出错: {str(e)}")
        return False

if __name__ == "__main__":
    args = parse_arguments()
    send_test_message(args) 