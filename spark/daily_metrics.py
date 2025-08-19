from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import *
from datetime import datetime

daily_user_metrics_schema = StructType([
    StructField("user_id", IntegerType(), False), 
    StructField("date", DateType(), False),     

    StructField("daily_total_visits", IntegerType(), True),
    StructField("daily_unique_product_views", IntegerType(), True),
    StructField("daily_total_add_to_cart", IntegerType(), True),
    StructField("daily_total_purchases", IntegerType(), True), 
    StructField("daily_total_items_purchased", IntegerType(), True), 
    StructField("daily_total_spend", DoubleType(), True),
    StructField("daily_category_activity", MapType(StringType(), IntegerType()), True),
    StructField("daily_brand_activity", MapType(StringType(), IntegerType()), True),  
    StructField("daily_active_session_count", IntegerType(), True),

    StructField("daily_support_interactions", IntegerType(), True),
    StructField("daily_calls", IntegerType(), True),
    StructField("daily_chats", IntegerType(), True),
    StructField("daily_tickets_opened", IntegerType(), True),
    StructField("daily_tickets_resolved", IntegerType(), True),
    StructField("daily_tickets_unresolved", IntegerType(), True), 
    StructField("daily_avg_satisfaction_score", DoubleType(), True),
    StructField("daily_frequent_issue_category", StringType(), True),

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

    result = result.withColumn("user_id", col("user_id").cast(LongType()))
    return result
    
if __name__ == "__main__":
    spark = SparkSession.builder \
       .appName("daily_metrics") \
       .master("spark://spark-master:7077") \
       .getOrCreate()

    snapshot_date = datetime.strptime(sys.argv[1],"%Y-%m-%d").date()
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
    
    # for month in [11]:
    #     for day in range(3,32):
    #         snapshot_date = datetime.strptime(f"2019-{month}-{day}","%Y-%m-%d").date()
    #         year,month,day = snapshot_date.year,str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)
    #         event_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_event/year={year}/month={month}/day={day}")
    #         support_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_support/year={year}/month={month}/day={day}")

    #         daily_user_metrics_df = daily_user_metrics(event_logs,support_logs)
    #         daily_user_metrics_df.write.mode("append")\
    #                 .format('jdbc')\
    #                 .option('url', 'jdbc:postgresql://data-warehouse:5432/mydatabase')\
    #                 .option('dbtable',"daily_user_metrics")\
    #                 .option('user','mydatabase')\
    #                 .option('password','mydatabase')\
    #                 .option('driver','org.postgresql.Driver')\
    #                 .save()
