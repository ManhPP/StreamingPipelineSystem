from pyspark.sql import SparkSession

conf = {
    "app_name": "StreamingPipelineApplication",
    "broker": "kafka1:19091",
    "topic": "1_RAW_tweets",
    "query_name": "q1",
    "path": "hdfs://namenode:8020/tmp/data/covid",
    "checkpoint_location": "hdfs://namenode:8020/tmp/checkpoints/covid"
}
spark = SparkSession\
       .builder\
       .appName(conf["app_name"])\
       .config("spark.streaming.stopGracefullyOnShutdown", "true")\
       .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create DataSet representing the stream of input lines from kafka
lines = spark\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers", conf["broker"])\
   .option("subscribe", conf["topic"])\
   .option("startingOffsets", "earliest") \
   .load()\
   .selectExpr("CAST(value AS STRING)")

print("======================")
# Start running the query that prints the running counts to the console
query = lines\
   .writeStream\
   .format('console')\
   .start()

lines.writeStream \
  .queryName(conf["query_name"]) \
  .outputMode("append") \
  .format("json") \
  .option("path", conf["path"]) \
  .option("checkpointLocation", conf["checkpoint_location"]) \
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
