from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
#,event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session

json_schema = StructType([
    StructField("event_time",TimestampType(),True),
    StructField("event_type", StringType(),True),
    StructField("product_id",LongType(),True),
    StructField("category_id",LongType(),True),
    StructField("category_code", StringType(),True),
    StructField("brand",StringType(),True),
    StructField("price", DoubleType(),True),
    StructField("user_id",LongType(),True),
    StructField("user_session",StringType(),True)
])

def stream_to_hdfs():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-0:9092,kafka-1:9092,kafka-2:9092")\
        .option("failOnDataLoss", "false") \
        .option("includeHeaders", "true") \
        .option("subscribePattern", f"logs_data_.*") \
        .load()
    json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")
    json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")
    json_expanded_df = json_expanded_df.withColumn("year", year(col("event_time")))
    json_expanded_df = json_expanded_df.withColumn("month", month(col("event_time")))
    json_expanded_df = json_expanded_df.withColumn("day", date_format(col("event_time"), "dd"))
    
    query = json_expanded_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet")\
        .option("path",f"hdfs://namenode:9000/tmp/")\
        .option("checkpointLocation", f"hdfs://namenode:9000/check-point-tmp/")\
        .partitionBy("year", "month", "day") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    stream_to_hdfs()