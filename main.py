from pyspark.sql import SparkSession

spark = SparkSession\
       .builder\
       .appName("StructuredKafka")\
       .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create DataSet representing the stream of input lines from kafka
lines = spark\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers", "172.23.0.10:19091")\
   .option("subscribe", "topic1")\
   .load()\
   .selectExpr("CAST(value AS STRING)")

print("======================")
# Start running the query that prints the running counts to the console
query = lines\
   .writeStream\
   .format('console')\
   .start()

lines.writeStream \
  .queryName("Persist the raw data of Taxi Rides") \
  .outputMode("append") \
  .format("json") \
  .option("path", "hdfs://namenode:8020/tmp/datalake/RidesRaw") \
  .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoints/RidesRaw") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()
print("**********************")

query.awaitTermination()
# spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /opt/spark-data/test_pyspark.py
# spark.read.json("hdfs://namenode:8020/tmp/datalake/RidesRaw")
