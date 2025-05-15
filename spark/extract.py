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
        .option("subscribe", "logs_data_10, logs_data_11, stop-signal-topic") \
        .load()
    
    json_df = df.filter(col("topic").isin("logs_data_10","logs_data_11"))\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")
    json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")
    json_expanded_df = json_expanded_df.withColumn("year", year(col("event_time")))
    json_expanded_df = json_expanded_df.withColumn("month", month(col("event_time")))
    json_expanded_df = json_expanded_df.withColumn("day", date_format(col("event_time"), "dd"))
    
    query = json_expanded_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet")\
        .option("path","hdfs://namenode:9000/tmp/")\
        .option("checkpointLocation", "hdfs://namenode:9000/check-point-tmp/")\
        .partitionBy("year", "month", "day") \
        .start()

    # stop_signal_df = df.filter(col("topic") == "stop-signal-topic") \
    #                 .selectExpr("CAST(value AS STRING) as stop_signal")
    
    # # Ghi tín hiệu dừng vào memory sink
    # stop_signal_query = stop_signal_df.writeStream \
    #     .outputMode("append") \
    #     .format("memory") \
    #     .queryName("stop_signal_mem_query") \
    #     .start()
    
    # def check_stop_signal():
    #     while query.isActive:
    #         time.sleep(5)
    #         try:
    #             stop_df = spark.sql("SELECT stop_signal FROM stop_signal_mem_query WHERE stop_signal = 'stop'")
    #             if stop_df.count() > 0:
    #                 print("Stop signal received. Will let Spark finish current batch...")
    #                 # Không dừng đột ngột mà cho phép Spark kết thúc tự nhiên
    #                 return
    #         except Exception as e:
    #             print("Error checking stop signal:", e)

    # stop_thread = threading.Thread(target=check_stop_signal, daemon=True)
    # stop_thread.start()

    # Cho phép Spark Streaming tiếp tục chạy đến khi được yêu cầu dừng
    query.awaitTermination()
    print("STREAM_SUCCESS")
    # stop_signal_query.awaitTermination()


if __name__ == "__main__":
    stream_to_hdfs()