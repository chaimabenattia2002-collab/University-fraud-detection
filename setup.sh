#!/bin/bash

# University Fraud Detection System - Setup Script
# This script sets up and runs the demo

echo "================================================================================"
echo "🎓 UNIVERSITY FRAUD DETECTION SYSTEM - SETUP"
echo "================================================================================"
echo ""

# Check Python
echo "✓ Checking Python installation..."
python3 --version
if [ $? -ne 0 ]; then
    echo "❌ Python 3 not found. Please install Python 3.8 or higher"
    exit 1
fi

# Check Java
echo "✓ Checking Java installation..."
java -version
if [ $? -ne 0 ]; then
    echo "❌ Java not found. Please install Java 11"
    exit 1
fi

# Check PySpark
echo "✓ Checking PySpark installation..."
python3 -c "import pyspark; print('PySpark version:', pyspark.__version__)" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ PySpark not found"
    echo "Installing PySpark..."
    pip3 install pyspark
fi

# Install dependencies
echo ""
echo "Installing required packages..."
pip3 install faker 2>/dev/null

echo ""
echo "================================================================================"
echo "✅ SETUP COMPLETE!"
echo "================================================================================"
echo ""
echo "Choose how to run the system:"
echo ""
echo "1. QUICK DEMO (Recommended) - No Kafka needed:"
echo "   python3 fraud_detection_demo.py"
echo ""
echo "2. FULL SYSTEM with Kafka:"
echo "   Terminal 1: python3 kafka_producer.py"
echo "   Terminal 2: python3 spark_consumer.py"
echo ""
echo "================================================================================"
echo ""

# Ask user if they want to run the demo
read -p "Run the quick demo now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo ""
    echo "Starting demo..."
    sleep 2
    python3 fraud_detection_demo.py
fi
