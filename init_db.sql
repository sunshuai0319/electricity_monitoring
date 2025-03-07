-- 酒店用电异常监控系统数据库初始化脚本

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS hotel_monitoring DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE hotel_monitoring;

-- 智能电表数据表(如果已存在，则仅供参考)
CREATE TABLE IF NOT EXISTS smart_meter (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    hotel_id VARCHAR(50) NOT NULL,
    room_number VARCHAR(20) NOT NULL,
    reading_value DECIMAL(10,2) NOT NULL,
    script_time DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hotel_room (hotel_id, room_number),
    INDEX idx_script_time (script_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='智能电表数据表';

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用电监控记录表';

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
    INDEX idx_hotel_room (hotel_id, room_number),
    INDEX idx_detected_time (detected_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用电异常记录表';

-- 更新现有表结构（如果表已存在）
-- 为 electricity_monitoring 表添加 msg_type 字段（如果不存在）
ALTER TABLE electricity_monitoring 
ADD COLUMN IF NOT EXISTS actual_checkin_time DATETIME NULL AFTER expected_checkout_time,
ADD COLUMN IF NOT EXISTS actual_checkout_time DATETIME NULL AFTER actual_checkin_time,
ADD COLUMN IF NOT EXISTS msg_type VARCHAR(20) DEFAULT 'checkin' COMMENT '消息类型：checkin-入住监控，checkout-离店监控' AFTER monitoring_start_time;

-- 为 electricity_abnormal 表添加 msg_type 字段（如果不存在）
ALTER TABLE electricity_abnormal 
ADD COLUMN IF NOT EXISTS msg_type VARCHAR(20) DEFAULT 'checkin' COMMENT '消息类型：checkin-入住监控，checkout-离店监控' AFTER is_notified;

-- 插入测试数据（根据需要选择是否使用）
-- INSERT INTO smart_meter (hotel_id, room_number, reading_value, script_time) VALUES
-- ('H001', '101', 1000.50, '2023-04-10 14:00:00'),
-- ('H001', '101', 1001.20, '2023-04-10 14:30:00'),
-- ('H001', '101', 1001.80, '2023-04-10 15:00:00'),
-- ('H001', '101', 1002.50, '2023-04-10 15:30:00'),
-- ('H001', '101', 1003.30, '2023-04-10 16:00:00'),
-- ('H001', '101', 1004.20, '2023-04-10 16:30:00'),
-- ('H001', '101', 1005.10, '2023-04-10 17:00:00'),
-- ('H001', '101', 1006.00, '2023-04-10 17:30:00'),
-- ('H001', '101', 1007.00, '2023-04-10 18:00:00'),
-- ('H001', '101', 1008.00, '2023-04-10 18:30:00');

-- 记得为应用创建专用数据库用户（生产环境使用）
-- CREATE USER 'hotel_monitor'@'localhost' IDENTIFIED BY 'your_strong_password';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON hotel_monitoring.* TO 'hotel_monitor'@'localhost';
-- FLUSH PRIVILEGES; 