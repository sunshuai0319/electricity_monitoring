-- 酒店用电异常监控系统数据库初始化脚本

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS zxtf_saas_test DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE zxtf_saas_test;

-- 用电监控记录表
CREATE TABLE IF NOT EXISTS electricity_monitoring (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    dept_code VARCHAR(50) NOT NULL COMMENT '部门编码',
    room_number VARCHAR(20) NOT NULL COMMENT '房间号',
    check_in_time DATETIME COMMENT '入住时间',
    expected_checkout_time DATETIME COMMENT '预计离店时间',
    actual_checkin_time DATETIME COMMENT '实际入住时间',
    actual_checkout_time DATETIME COMMENT '实际离店时间',
    initial_reading DECIMAL(10,2) COMMENT '初始电表读数',
    monitoring_start_time DATETIME COMMENT '监控开始时间',
    msg_type VARCHAR(20) DEFAULT 'checkin' COMMENT '消息类型：checkin-入住监控，checkout-离店监控',
    product_key VARCHAR(100) COMMENT '产品密钥',
    device_name VARCHAR(100) DEFAULT '未知设备' COMMENT '设备名称',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dept_room (dept_code, room_number),
    INDEX idx_monitoring_start (monitoring_start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用电监控记录表';

-- 用电异常记录表(辅表)
CREATE TABLE IF NOT EXISTS electricity_abnormal (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    dept_code VARCHAR(50) NOT NULL COMMENT '部门编码',
    room_number VARCHAR(20) NOT NULL COMMENT '房间号',
    detected_time DATETIME NOT NULL COMMENT '检测时间',
    total_consumption DECIMAL(10,2) NOT NULL COMMENT '总消耗电量',
    continuous_periods INT NOT NULL COMMENT '连续异常周期数',
    is_notified BOOLEAN DEFAULT FALSE COMMENT '是否已通知',
    msg_type VARCHAR(20) DEFAULT 'checkin' COMMENT '消息类型：checkin-入住监控，checkout-离店监控',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dept_room (dept_code, room_number),
    INDEX idx_detected_time (detected_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用电异常记录表(辅表)';
