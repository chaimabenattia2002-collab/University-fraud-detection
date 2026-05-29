#!/bin/bash

echo "================================================================================"
echo "🎓 UNIVERSITY FRAUD DETECTION SYSTEM - KAFKA VERSION"
echo "================================================================================"
echo ""

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

run_in_new_terminal() {
    osascript -e "tell application \"Terminal\" to do script \"cd /usr/local/kafka && $1\""
}

echo "1️⃣  Starting Zookeeper..."
run_in_new_terminal "bin/zookeeper-server-start.sh config/zookeeper.properties"
sleep 5

echo "2️⃣  Starting Kafka Server..."
run_in_new_terminal "bin/kafka-server-start.sh config/server.properties"
sleep 10

echo "3️⃣  Verifying Kafka topic..."
cd /usr/local/kafka
bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep university-events > /dev/null
if [ $? -ne 0 ]; then
    bin/kafka-topics.sh --create --topic university-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
fi

echo "4️⃣  Starting Spark Consumer (Clean Version)..."
run_in_new_terminal "cd '$SCRIPT_DIR' && python3 spark_consumer_clean.py"
sleep 5

echo "5️⃣  Starting Kafka Producer..."
run_in_new_terminal "cd '$SCRIPT_DIR' && python3 kafka_producer.py"

echo ""
echo "================================================================================"
echo "✅ ALL COMPONENTS STARTED!"
echo "================================================================================"
