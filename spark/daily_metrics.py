from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import *
from datetime import datetime

daily_user_metrics_schema = StructType([
    StructField("user_id", IntegerType(), False), # Key chính của user
    StructField("date", DateType(), False),      # Ngày của các metrics này

    # --- Metrics từ Event Logs ---
    StructField("daily_total_visits", IntegerType(), True),
    StructField("daily_unique_product_views", IntegerType(), True),
    StructField("daily_total_add_to_cart", IntegerType(), True),
    StructField("daily_total_purchases", IntegerType(), True), # Số lượng order_id duy nhất
    StructField("daily_total_items_purchased", IntegerType(), True), # Sum of quantity across orders
    StructField("daily_total_spend", DoubleType(), True), # Sum of total_amount across orders
    StructField("daily_category_activity", MapType(StringType(), IntegerType()), True), # Category -> count
    StructField("daily_brand_activity", MapType(StringType(), IntegerType()), True),   # Brand -> count
    StructField("daily_active_session_count", IntegerType(), True),

    # --- Metrics từ Customer Support Logs ---
    StructField("daily_support_interactions", IntegerType(), True),
    StructField("daily_calls", IntegerType(), True),
    StructField("daily_chats", IntegerType(), True),
    StructField("daily_tickets_opened", IntegerType(), True),
    StructField("daily_tickets_resolved", IntegerType(), True),
    StructField("daily_tickets_unresolved", IntegerType(), True), # Xem xét lại cách tính cho chính xác
    StructField("daily_avg_satisfaction_score", DoubleType(), True),
    StructField("daily_frequent_issue_category", StringType(), True),
    
    StructField("last_update_timestamp", TimestampType(), True) # Thời gian job chạy và cập nhật bản ghi này
])
def daily_user_metrics(event_logs,support_logs):
    e_df = event_logs.groupBy("user_id", to_date("event_time").alias("date")).agg(
        count("*").alias("daily_total_event"),
        countDistinct(col("user_session")).alias("daily_total_visits"),
        count(when(col("event_type") == "view", True)).alias("daily_total_view"),
        count(when(col("event_type") == "cart", True)).alias("daily_total_add_to_cart"),
        count(when(col("event_type") == "purchase", True)).alias("daily_total_purchase"),
        sum(when(col("event_type") == "purchase", col("price"))).alias("daily_total_spend"))
    s_df = support_logs.groupBy("user_id", to_date("event_time").alias("date")).agg(
        count("*").alias("daily_total_interaction"),
        avg(col("satisfaction_score")).alias("daily_average_satisfaction")
    )
    result = e_df.join(s_df, on =["user_id","date"], how="outer")
    result = result.fillna(0, subset=[
        "daily_total_event",
        "daily_total_visits",
        "daily_total_spend",
        "daily_total_interaction",
        "daily_average_satisfaction"
    ])
    # Ép kiểu bằng selectExpr để khớp schema
    result = result.withColumn("user_id", col("user_id").cast(LongType()))

    return result

def daily_sys_metrics(event_logs):
    daily_event_metrics = event_logs.groupBy(to_date("event_time").alias("date")).agg(
        countDistinct("user_id").alias("active_users"),
        countDistinct(when(col("event_type") == "view", col("product_id"))).alias("daily_unique_product_views"),
        count(when(col("event_type") == "cart", True)).alias("daily_total_add_to_cart"),
        sum(when(col("event_type") == "purchase", lit(1))).alias("daily_total_purchases"),
        sum(when(col("event_type") == "purchase", col("price"))).alias("daily_total_spend"),
        countDistinct(col("user_session")).alias("daily_total_visits"),
)
    return
    
if __name__ == "__main__":
    spark = SparkSession.builder \
       .appName("daily_metrics") \
       .master("spark://spark-master:7077") \
       .getOrCreate()

    # snapshot_date = datetime.strptime(sys.argv[1],"%Y-%m-%d").date()
    # year,month,day = snapshot_date.year,str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)
    # event_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_event/year={year}/month={month}/day={day}")
    # support_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_support/year={year}/month={month}/day={day}")

    # daily_user_metrics = daily_user_metrics(event_logs,support_logs)
    # daily_user_metrics.write.mode("append")\
    #         .format('jdbc')\
    #         .option('url', 'jdbc:postgresql://data-warehouse:5432/mydatabase')\
    #         .option('dbtable',"daily_user_metrics")\
    #         .option('user','mydatabase')\
    #         .option('password','mydatabase')\
    #         .option('driver','org.postgresql.Driver')\
    #         .save()
    
    for month in [10]:
        for day in range(3,32):
            snapshot_date = datetime.strptime(f"2019-{month}-{day}","%Y-%m-%d").date()
            year,month,day = snapshot_date.year,str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)
            event_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_event/year={year}/month={month}/day={day}")
            support_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_support/year={year}/month={month}/day={day}")

            daily_user_metrics_df = daily_user_metrics(event_logs,support_logs)
            daily_user_metrics_df.write.mode("append")\
                    .format('jdbc')\
                    .option('url', 'jdbc:postgresql://data-warehouse:5432/mydatabase')\
                    .option('dbtable',"daily_user_metrics")\
                    .option('user','mydatabase')\
                    .option('password','mydatabase')\
                    .option('driver','org.postgresql.Driver')\
                    .save()
