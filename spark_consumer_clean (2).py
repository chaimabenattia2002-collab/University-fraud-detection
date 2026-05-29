import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf

os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
os.environ['PYSPARK_PYTHON'] = 'python3'

conf = SparkConf()
conf.set("spark.driver.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/javax.security.auth=ALL-UNNAMED")

spark = SparkSession.builder.appName("FraudDetection").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8").config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("student_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True),
    StructField("exam_id", StringType(), True),
])

print("\n" + "="*80)
print("🎓 UNIVERSITY FRAUD DETECTION - REAL-TIME MONITORING")
print("="*80)
print("Listening for events from Kafka...")
print("="*80 + "\n")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "university-events").option("startingOffsets", "earliest").load()
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    print(f"📊 Batch {batch_id}: Processing {batch_df.count()} events...")
    
    fraud_detected = False
    
    # FRAUD RULE 1: Multiple Locations
    logins = batch_df.filter(col("event_type") == "login")
    if logins.count() > 0:
        fraud1 = logins.groupBy("student_id").agg(
            countDistinct("ip_address").alias("unique_ips"),
            collect_set("ip_address").alias("ip_list"),
            collect_set("location").alias("locations")
        ).filter(col("unique_ips") >= 3)
        
        if fraud1.count() > 0:
            fraud_detected = True
            print("\n" + "🚨"*40)
            print("⚠️  FRAUD ALERT: MULTIPLE LOCATION LOGINS")
            print("🚨"*40)
            print(f"Detected {fraud1.count()} students logging in from multiple locations:\n")
            for row in fraud1.collect():
                print(f"  👤 Student: {row.student_id}")
                print(f"     📍 Locations: {row.locations}")
                print(f"     🌐 IP Addresses ({row.unique_ips}): {row.ip_list}")
                print()
    
    # FRAUD RULE 2: Exam Cheating
    exams = batch_df.filter(col("event_type") == "exam_access")
    if exams.count() > 0:
        fraud2 = exams.groupBy("ip_address", "exam_id").agg(
            countDistinct("student_id").alias("student_count"),
            collect_set("student_id").alias("students")
        ).filter(col("student_count") >= 2)
        
        if fraud2.count() > 0:
            fraud_detected = True
            print("\n" + "🚨"*40)
            print("⚠️  FRAUD ALERT: EXAM CHEATING DETECTED")
            print("🚨"*40)
            print(f"Detected {fraud2.count()} cases of multiple students from same IP:\n")
            for row in fraud2.collect():
                print(f"  📝 Exam: {row.exam_id}")
                print(f"     🌐 Shared IP: {row.ip_address}")
                print(f"     👥 Students ({row.student_count}): {row.students}")
                print()
    
    # FRAUD RULE 3: Account Sharing
    if logins.count() > 0:
        fraud3 = logins.groupBy("student_id").agg(
            countDistinct("device").alias("device_count"),
            collect_set("device").alias("devices")
        ).filter(col("device_count") >= 3)
        
        if fraud3.count() > 0:
            fraud_detected = True
            print("\n" + "🚨"*40)
            print("⚠️  FRAUD ALERT: ACCOUNT SHARING DETECTED")
            print("🚨"*40)
            print(f"Detected {fraud3.count()} students using multiple devices:\n")
            for row in fraud3.collect():
                print(f"  👤 Student: {row.student_id}")
                print(f"     💻 Devices ({row.device_count}): {row.devices}")
                print()
    
    if fraud_detected:
        print("="*80 + "\n")
    else:
        print("✅ No fraud detected in this batch\n")

query = events.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()
