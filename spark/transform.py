from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
import sys

spark = SparkSession.builder \
        .appName("Transformation") \
        .master("spark://spark-master:7077") \
        .config("spark.jars","/opt/airflow/code/postgresql-42.2.5.jar")\
        .getOrCreate()

def compute_static_customer_profile(df,snapshot_date):
    
    df_profile = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id").agg(
            min("event_time").alias("first_purchase_date"),
            max("event_time").alias("last_purchase_date"),
            round(sum("price"), 2).alias("total_spent"),
            count("product_id").alias("total_orders")
        )
# Phân loại khách hàng theo tổng chi tiêu
    df_profile = df_profile.withColumn("customer_tier",
        when(col("total_spent") >= 10000000, "VIP")
        .when(col("total_spent") >= 3000000, "Medium")
        .otherwise("Low"))\
        .withColumn("snapshot_date",lit(snapshot_date))
    
    return df_profile
# top category mua nhiều nhất
def compute_top_category(df,snapshot_date):
    df_category = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id", "category_id", "category_code") \
        .agg(count("*").alias("num_purchase"))
    window_category = Window.partitionBy("user_id").orderBy(desc("num_purchase"))
    df_preferred_category = df_category.withColumn("rank", rank().over(window_category))\
                                        .withColumn("snapshot_date",lit(snapshot_date))
    return df_preferred_category

def compute_top_brand(df,snapshot_date):
#top brand mua nhieu nhat
    df_brand = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id", "brand") \
        .agg(count("*").alias("num_purchase"))
    window_brand = Window.partitionBy("user_id").orderBy(desc("num_purchase"))
    df_preferred_brand = df_brand.withColumn("rank", rank().over(window_brand))\
                                .withColumn("snapshot_date",lit(snapshot_date))
    return df_preferred_brand

def compute_churn_risk(df, snapshot_date):
    df_churn = df.groupBy("user_id").agg(
        min(col("event_time")).alias("first_activity_date"),
        max(when(col("event_type") == "purchase", col("event_time"))).alias("last_purchase_date"),
        max(when(col("event_type").isin("view","cart"), col("event_time"))).alias("last_active_day")
    )

    df_churn = df_churn.withColumn("days_since_last_purchase", datediff(lit(snapshot_date), col("last_purchase_date")))\
                       .withColumn("days_since_last_activity", datediff(lit(snapshot_date), col("last_active_day")))

    df_churn = df_churn.withColumn("churn_risk",
        when(col("days_since_last_purchase") > 30, "High")
        .when(col("days_since_last_activity") > 60, "High")
        .when((col("days_since_last_purchase") > 15) & (col("days_since_last_activity") < 30), "Medium")
        .otherwise("Low"))\
        .select("user_id","chunk_risk")
    return df_churn

def process(snapshot_date):
    base_path = "hdfs://namenode:9000/tmp/year=2019"
    all_paths = []
    for month in [10, 11]:
        for day in range(1, 32):
            try:
                day_date = datetime(2019, month, day).date()
                if day_date <= snapshot_date:
                    m = str(month).zfill(2)
                    d = str(day).zfill(2)
                    all_paths.append(f"{base_path}/month={m}/day={d}")
            except:
                continue
    # print(all_paths)
    df = spark.read.option("basePath", base_path).parquet(*all_paths)
    df_profile = compute_static_customer_profile(df, snapshot_date)
    df_top_category = compute_top_category(df, snapshot_date)
    df_top_brand = compute_top_brand(df, snapshot_date)
    df_churn = compute_churn_risk(df, snapshot_date)
    
    df_profile.write.mode("append").partitionBy("snapshot_date").parquet("hdfs://namenode:9000/staging/customer_profile")
    df_top_category.write.mode("append").partitionBy("snapshot_date").parquet("hdfs://namenode:9000/staging/customer_prefer_category")
    df_top_brand.write.mode("append").partitionBy("snapshot_date").parquet("hdfs://namenode:9000/staging/customer_prefer_brand")
    df_churn.write.mode("append").partitionBy("snapshot_date").parquet("hdfs://namenode:9000/staging/customer_churn_risk")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit transform.py <YYYY-MM-DD>")
        sys.exit(1)
    try:
        snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError:
        print("Error: Date format should be YYYY-MM-DD")
        sys.exit(1)

    process(snapshot_date)



