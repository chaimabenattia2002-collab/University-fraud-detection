"""
University Fraud Detection - Kafka Event Producer
Generates simulated university events and sends them to Kafka
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample data
students = [f"STU{1000 + i}" for i in range(50)]
courses = ["CS101", "CS102", "MATH201", "PHYS101", "ENG101"]
exams = ["EXAM_CS101_MID", "EXAM_CS102_FINAL", "EXAM_MATH201_MID"]
ips = [f"192.168.1.{i}" for i in range(1, 255)]
devices = ["Windows_PC", "MacBook", "iPhone", "Android", "iPad", "Linux_PC"]

def generate_login():
    """Generate a student login event"""
    return {
        'event_type': 'login',
        'student_id': random.choice(students),
        'timestamp': datetime.now().isoformat(),
        'ip_address': random.choice(ips),
        'device': random.choice(devices),
        'location': fake.city()
    }

def generate_exam_access():
    """Generate an exam access event"""
    return {
        'event_type': 'exam_access',
        'student_id': random.choice(students),
        'exam_id': random.choice(exams),
        'timestamp': datetime.now().isoformat(),
        'ip_address': random.choice(ips),
        'device': random.choice(devices)
    }

def generate_attendance():
    """Generate an attendance event"""
    return {
        'event_type': 'attendance',
        'student_id': random.choice(students),
        'course_id': random.choice(courses),
        'timestamp': datetime.now().isoformat(),
        'ip_address': random.choice(ips)
    }

def generate_fraud_pattern():
    """Generate intentional fraud for testing (3 types only)"""
    fraud_type = random.choice(['multiple_locations', 'exam_cheating', 'account_sharing'])
    
    if fraud_type == 'multiple_locations':
        # Same student, different IPs and locations
        student = random.choice(students)
        events = []
        for i in range(4):
            events.append({
                'event_type': 'login',
                'student_id': student,
                'timestamp': datetime.now().isoformat(),
                'ip_address': ips[i],
                'device': random.choice(devices),
                'location': fake.city()
            })
        return events
    
    elif fraud_type == 'exam_cheating':
        # Multiple students, same IP
        ip = random.choice(ips)
        events = []
        for i in range(3):
            events.append({
                'event_type': 'exam_access',
                'student_id': students[i],
                'exam_id': random.choice(exams),
                'timestamp': datetime.now().isoformat(),
                'ip_address': ip,
                'device': random.choice(devices)
            })
        return events
    
    else:  # account_sharing
        # Same student, multiple devices
        student = random.choice(students)
        events = []
        for i in range(4):
            events.append({
                'event_type': 'login',
                'student_id': student,
                'timestamp': datetime.now().isoformat(),
                'ip_address': random.choice(ips),
                'device': devices[i],
                'location': fake.city()
            })
        return events

# Main production loop
print("="*60)
print("UNIVERSITY FRAUD DETECTION - EVENT PRODUCER")
print("="*60)
print("\nSending 200 events to Kafka...")
print("Fraud types: Multiple Locations, Exam Cheating, Account Sharing")
print("-"*60)

event_count = 0
target_events = 200

while event_count < target_events:
    # 30% fraud rate
    if random.random() < 0.3:
        events = generate_fraud_pattern()
        for event in events:
            producer.send('university-events', event)
            event_count += 1
            print(f"[FRAUD] {event['event_type']}: {event.get('student_id')}")
            if event_count >= target_events:
                break
        time.sleep(0.5)
    else:
        # Normal event
        event_func = random.choice([generate_login, generate_exam_access, generate_attendance])
        event = event_func()
        producer.send('university-events', event)
        event_count += 1
        print(f"[{event_count}] {event['event_type']}: {event.get('student_id')}")
    
    time.sleep(random.uniform(0.1, 0.5))

producer.flush()
print("\n" + "="*60)
print(f"✅ Successfully sent {event_count} events!")
print("="*60)
