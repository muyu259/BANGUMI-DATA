#!/bin/bash

# Flume + Kafka 数据采集系统启动脚本
# 文件路径: /path/to/your/bangumidata/start-data-pipeline.sh

# 配置变量 (请根据实际环境修改)
KAFKA_HOME="/opt/kafka"
FLUME_HOME="/opt/flume"
PROJECT_DIR="/path/to/your/bangumidata"
JSON_DIR="$PROJECT_DIR/json"
LOG_DIR="$PROJECT_DIR/logs"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_DIR/pipeline.log"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_DIR/pipeline.log"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_DIR/pipeline.log"
}

# 检查服务状态
check_service_status() {
    local service_name=$1
    local process_pattern=$2
    
    if pgrep -f "$process_pattern" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# 启动Zookeeper
start_zookeeper() {
    log_info "启动Zookeeper..."
    
    if check_service_status "Zookeeper" "zookeeper"; then
        log_warn "Zookeeper已在运行"
        return 0
    fi
    
    nohup "$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties" > "$LOG_DIR/zookeeper.log" 2>&1 &
    sleep 5
    
    if check_service_status "Zookeeper" "zookeeper"; then
        log_info "Zookeeper启动成功"
    else
        log_error "Zookeeper启动失败"
        return 1
    fi
}

# 启动Kafka
start_kafka() {
    log_info "启动Kafka..."
    
    if check_service_status "Kafka" "kafka"; then
        log_warn "Kafka已在运行"
        return 0
    fi
    
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" > "$LOG_DIR/kafka.log" 2>&1 &
    sleep 10
    
    if check_service_status "Kafka" "kafka"; then
        log_info "Kafka启动成功"
    else
        log_error "Kafka启动失败"
        return 1
    fi
}

# 启动Flume
start_flume() {
    log_info "启动Flume Agent..."
    
    if check_service_status "Flume" "flume-ng"; then
        log_warn "Flume已在运行"
        return 0
    fi
    
    nohup "$FLUME_HOME/bin/flume-ng" agent \
        --conf "$FLUME_HOME/conf" \
        --conf-file "$PROJECT_DIR/flume-kafka-agent.conf" \
        --name agent > "$LOG_DIR/flume.log" 2>&1 &
    sleep 5
    
    if check_service_status "Flume" "flume-ng"; then
        log_info "Flume启动成功"
    else
        log_error "Flume启动失败"
        return 1
    fi
}

# 停止所有服务
stop_all() {
    log_info "停止所有服务..."
    
    # 停止Flume
    if check_service_status "Flume" "flume-ng"; then
        pkill -f "flume-ng" || true
        log_info "Flume已停止"
    fi
    
    # 停止Kafka
    if check_service_status "Kafka" "kafka"; then
        "$KAFKA_HOME/bin/kafka-server-stop.sh" || pkill -f "kafka" || true
        log_info "Kafka已停止"
    fi
    
    # 停止Zookeeper
    if check_service_status "Zookeeper" "zookeeper"; then
        "$KAFKA_HOME/bin/zookeeper-server-stop.sh" || pkill -f "zookeeper" || true
        log_info "Zookeeper已停止"
    fi
    
    log_info "所有服务已停止"
}

# 查看服务状态
status() {
    log_info "检查服务状态..."
    
    if check_service_status "Zookeeper" "zookeeper"; then
        echo -e "${GREEN}✓${NC} Zookeeper 运行中"
    else
        echo -e "${RED}✗${NC} Zookeeper 未运行"
    fi
    
    if check_service_status "Kafka" "kafka"; then
        echo -e "${GREEN}✓${NC} Kafka 运行中"
    else
        echo -e "${RED}✗${NC} Kafka 未运行"
    fi
    
    if check_service_status "Flume" "flume-ng"; then
        echo -e "${GREEN}✓${NC} Flume 运行中"
    else
        echo -e "${RED}✗${NC} Flume 未运行"
    fi
}

# 创建Topic
create_topic() {
    log_info "创建Kafka Topic: bangumidata-topic"
    
    if "$KAFKA_HOME/bin/kafka-topics.sh" --create \
        --topic bangumidata-topic \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1; then
        log_info "Topic创建成功"
    else
        log_warn "Topic可能已存在"
    fi
}

# 查看Kafka消息
consume_messages() {
    log_info "启动Kafka消费者监听bangumidata-topic"
    "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
        --topic bangumidata-topic \
        --bootstrap-server localhost:9092 \
        --from-beginning
}

# 查看Flume日志
tail_flume_logs() {
    tail -f "$LOG_DIR/flume.log"
}

# 测试数据采集
test_data_collection() {
    log_info "测试数据采集功能..."
    
    # 创建测试JSON文件
    local test_file="$JSON_DIR/test_$(date +%s).json"
    cat > "$test_file" << EOF
{
    "test": true,
    "timestamp": "$(date -Iseconds)",
    "message": "This is a test message for data collection"
}
EOF
    
    log_info "创建测试文件: $test_file"
    sleep 2
    
    # 检查文件是否被处理
    log_info "等待Flume处理文件..."
    sleep 5
    
    # 清理测试文件
    rm -f "$test_file"
    log_info "测试完成，请查看Kafka消费者输出"
}

# 显示帮助信息
show_help() {
    echo "Flume + Kafka 数据采集系统管理脚本"
    echo ""
    echo "用法: $0 [命令]"
    echo ""
    echo "命令:"
    echo "  start         启动所有服务"
    echo "  stop          停止所有服务"
    echo "  restart       重启所有服务"
    echo "  status        查看服务状态"
    echo "  topic         创建Kafka Topic"
    echo "  consume       启动Kafka消费者"
    echo "  test          测试数据采集"
    echo "  logs          查看Flume日志"
    echo "  help          显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 start"
    echo "  $0 status"
    echo "  $0 consume"
}

# 主函数
main() {
    case "${1:-start}" in
        "start")
            log_info "启动数据采集系统..."
            start_zookeeper || exit 1
            start_kafka || exit 1
            create_topic
            start_flume || exit 1
            log_info "数据采集系统启动完成！"
            ;;
        "stop")
            stop_all
            ;;
        "restart")
            log_info "重启数据采集系统..."
            stop_all
            sleep 3
            start_zookeeper || exit 1
            start_kafka || exit 1
            start_flume || exit 1
            log_info "数据采集系统重启完成！"
            ;;
        "status")
            status
            ;;
        "topic")
            create_topic
            ;;
        "consume")
            consume_messages
            ;;
        "test")
            test_data_collection
            ;;
        "logs")
            tail_flume_logs
            ;;
        "help")
            show_help
            ;;
        *)
            echo "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"