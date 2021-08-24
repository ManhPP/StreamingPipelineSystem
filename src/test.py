from pyspark.sql import SparkSession

spark = SparkSession\
       .builder\
       .appName("test")\
       .getOrCreate()

a = spark.read.text("hdfs://namenode:8020/tmp/checkpoints/sps/offsets")
a.show()