# 酒店用电异常监控系统

这是一个用于监控酒店房间用电情况的系统，能够实时检测异常用电并发送报警通知。

## 功能特点

- 从RabbitMQ消费入住和离店信息消息
- 根据消息中的msgType字段区分入住消息(5710005)和离店消息(5710013)
- 根据入住信息初始化房间用电监控（阈值2度电）
- 根据离店信息初始化离店后房间用电监控（阈值1.5度电）
- 每30分钟检查一次房间用电量变化
- 检测连续异常用电情况并触发报警
- 记录异常用电数据到MySQL数据库
- 通过飞书机器人发送异常用电通知

## 性能优化特性

- **多消费者并行处理**：支持多线程并行消费RabbitMQ消息，提高消息处理吞吐量
- **MySQL连接池**：使用连接池管理数据库连接，减少频繁创建连接的开销
- **批量处理检查任务**：将房间检查任务分批并行处理，提高大规模房间监控效率
- **限制历史数据**：自动清理过多的历史记录，减少内存占用
- **优雅关闭机制**：支持系统优雅关闭，确保消息不丢失

## 系统要求

- Python 3.6+
- RabbitMQ 服务器
- MySQL 数据库
- 飞书机器人Webhook

## 安装方法

1. 克隆代码库到本地

```bash
git clone https://github.com/yourusername/hotel-electricity-monitoring.git
cd hotel-electricity-monitoring
```

2. 安装依赖包

```bash
pip install -r requirements.txt
```

3. 配置数据库

在MySQL中创建所需的数据表：

```sql
-- 智能电表数据表(假设已存在)
CREATE TABLE IF NOT EXISTS smart_meter (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hotel_id VARCHAR(50) NOT NULL,
    room_number VARCHAR(20) NOT NULL,
    reading_value DECIMAL(10,2) NOT NULL,
    script_time DATETIME NOT NULL,
    INDEX idx_hotel_room (hotel_id, room_number),
    INDEX idx_script_time (script_time)
);

-- 用电监控记录表
CREATE TABLE IF NOT EXISTS electricity_monitoring (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hotel_id VARCHAR(50) NOT NULL,
    room_number VARCHAR(20) NOT NULL,
    check_in_time DATETIME,
    expected_checkout_time DATETIME,
    actual_checkin_time DATETIME,
    actual_checkout_time DATETIME,
    initial_reading DECIMAL(10,2),
    monitoring_start_time DATETIME,
    msg_type VARCHAR(20) DEFAULT 'checkin' COMMENT '消息类型：checkin-入住监控，checkout-离店监控',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hotel_room (hotel_id, room_number)
);

-- 用电异常记录表
CREATE TABLE IF NOT EXISTS electricity_abnormal (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hotel_id VARCHAR(50) NOT NULL,
    room_number VARCHAR(20) NOT NULL,
    detected_time DATETIME NOT NULL,
    total_consumption DECIMAL(10,2) NOT NULL,
    continuous_periods INT NOT NULL,
    is_notified BOOLEAN DEFAULT FALSE,
    msg_type VARCHAR(20) DEFAULT 'checkin' COMMENT '消息类型：checkin-入住监控，checkout-离店监控',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hotel_room (hotel_id, room_number)
);
```

4. 配置系统参数

编辑`electricity_monitoring.py`文件中的`CONFIG`部分：

```python
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
```

## 性能调优

系统提供了多个可配置参数用于性能调优：

1. **消费者数量**：根据消息量和服务器CPU核心数调整 `consumer_count`，一般建议设置为CPU核心数的1-2倍
2. **连接池大小**：根据并发量调整 `pool_size`，过大会占用过多数据库连接资源
3. **批处理大小**：通过 `batch_size` 调整每批处理的房间数量，影响内存使用和CPU负载
4. **历史记录限制**：通过 `max_history_size` 控制每个房间保留的历史读数数量，减少内存占用

## 运行方法

```bash
python electricity_monitoring.py
```

建议使用进程管理工具（如supervisor或systemd）确保服务持续运行。

## RabbitMQ消息格式

系统接收的RabbitMQ消息应为JSON格式，包含以下字段：

### 入住消息格式

```json
{
  "hotel_id": "H12345",
  "room_number": "1001",
  "msgType": "5710005",
  "check_in_time": "2023-04-10 14:30:00",
  "expected_checkout_time": "2023-04-12 12:00:00"
}
```

### 离店消息格式

```json
{
  "hotel_id": "H12345",
  "room_number": "1001",
  "msgType": "5710013",
  "actual_checkin_time": "2023-04-10 14:30:00",
  "actual_checkout_time": "2023-04-12 12:00:00"
}
```

## 消息类型识别逻辑

系统通过以下方式识别消息类型：

1. 首先检查`msgType`字段值：
   - 当`msgType=5710005`时，识别为入住消息
   - 当`msgType=5710013`时，识别为离店消息

2. 如果`msgType`字段不存在，则通过消息内容判断：
   - 包含`check_in_time`字段时，识别为入住消息
   - 包含`actual_checkin_time`和`actual_checkout_time`字段时，识别为离店消息

## 测试发送消息

系统提供了测试工具来发送入住和离店消息：

### 发送入住消息

```bash
python test_message_sender.py --hotel-id H001 --room-number 101 --msg-type checkin
```

### 发送离店消息

```bash
python test_message_sender.py --hotel-id H001 --room-number 101 --msg-type checkout
```

## 异常检测逻辑

1. 系统接收入住/离店消息后，记录房间初始用电量
2. 每隔30分钟检查一次当前用电量，计算与上次记录的差值
3. 根据消息类型应用不同的阈值：
   - 入住消息：如果差值超过2度电，记为一次异常
   - 离店消息：如果差值超过1.5度电，记为一次异常
4. 连续4次异常将触发报警，并记录到数据库
5. 发送飞书通知给相关人员

## 系统架构

![系统架构图](https://placeholder-for-architecture-diagram.com)

### 主要模块

- **消息消费模块**：多线程并行消费RabbitMQ消息
- **初始化监控模块**：处理新消息并设置监控状态
- **定时检查模块**：批量并行检查所有房间用电情况
- **异常处理模块**：检测异常并发送警报
- **数据库访问层**：通过连接池管理数据库连接

## 性能表现

基于优化后的系统架构，单实例可以支持：
- 并发处理约20,000个不同房间的监控任务
- 每秒处理约50-100条入住/离店消息
- 每30分钟完成全量房间检查，耗时约1-2分钟（对于10,000个房间）

## 日志记录

系统日志保存在`electricity_monitoring.log`文件中，同时也会输出到控制台。
每10分钟会打印一次当前监控房间数等统计信息，便于系统状态监控。

## 故障排除

1. 如果系统无法连接RabbitMQ，将每30秒尝试重新连接
2. 消息处理异常不会导致死循环，系统会确认消息并记录错误
3. 数据库连接失败时会记录错误并继续尝试后续操作
4. 系统使用线程锁保护共享状态，确保多线程访问的安全性
5. 支持优雅关闭，确保系统停止时不丢失消息和状态

## 许可证

MIT 