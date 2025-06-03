from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

event_logs_schema = StructType([
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
support_logs_schema = StructType([
    StructField("event_time",TimestampType(),True),
    StructField("user_id", StringType(),True),
    StructField("interaction_type", StringType(),True),
    StructField("issue_category", StringType(),True),
    StructField("resolution_status", StringType(),True),
    StructField("satisfaction_score", DoubleType(),True)
])

def stream_to_hdfs():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-0:9092,kafka-1:9092,kafka-2:9092")\
        .option("failOnDataLoss", "false") \
        .option("includeHeaders", "true") \
        .option("subscribe", "logs_data_10, logs_data_11, customer_support_logs") \
        .load()
    
    event_df = df.filter(col("topic").isin("logs_data_10","logs_data_11"))\
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")
    event_expanded_df = event_df.withColumn("msg_value", from_json(event_df["msg_value"], event_logs_schema)).select("msg_value.*")
    event_expanded_df = event_expanded_df.withColumn("year", year(col("event_time")))\
                                        .withColumn("month", month(col("event_time")))\
                                        .withColumn("day", date_format(col("event_time"), "dd"))
                                    
    support_df = df.filter(col("topic") == "customer_support_logs")\
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value") \
                .select(from_json(col("value"), support_logs_schema).alias("data")) \
                .select("data.*")
    support_df = support_df.withColumn("year", year(col("event_time")))\
                            .withColumn("month", month(col("event_time")))\
                            .withColumn("day", date_format(col("event_time"), "dd"))

    query_event = event_expanded_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet")\
        .option("path","hdfs://namenode:9000/raw_event/")\
        .option("checkpointLocation", "hdfs://namenode:9000/check-point-tmp/")\
        .partitionBy("year", "month", "day") \
        .start()
    #spark sẽ không lưu các cột partition vào file dữ liệu
    query_support = support_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet")\
        .option("path","hdfs://namenode:9000/raw_support/")\
        .option("checkpointLocation", "hdfs://namenode:9000/check-point-support-logs/")\
        .partitionBy("year","month","day") \
        .start()

    print("Spark Streaming queries started for all sources...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    stream_to_hdfs()