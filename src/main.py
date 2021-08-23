from pyspark.sql import SparkSession

spark = SparkSession\
       .builder\
       .appName("StreamingPipelineApplication")\
       .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create DataSet representing the stream of input lines from kafka
lines = spark\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers", "kafka1:19091")\
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
  .queryName("Query 1") \
  .outputMode("append") \
  .format("json") \
  .option("path", "hdfs://namenode:8020/tmp/data/sps") \
  .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoints/sps") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()
print("**********************")

query.awaitTermination()
# spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /opt/spark-data/test_pyspark.py
# spark.read.json("hdfs://namenode:8020/tmp/datalake/RidesRaw") spark/bin/spark-submit --packages
# --master spark://spark-master:7077 --deploy-mode cluster
# --driver-memory 4g
# --executor-memory 2g
