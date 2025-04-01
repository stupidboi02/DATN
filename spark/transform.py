from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def compute_static_customer_profile(df):
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
        .otherwise("Low"))
    
# top category mua nhiều nhất
    df_category = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id", "category_id", "category_code") \
        .agg(count("*").alias("t"))
    window_category = Window.partitionBy("user_id").orderBy(desc("t"))
    df_preferred_category = df_category.withColumn("rank", rank().over(window_category)) \
        .filter(col("rank") <= 3).select("user_id","category_code")\
        .groupBy("user_id").agg(collect_list("category_code").alias("preferred_category"))


#top brand mua nhieu nhat
    df_brand = df.filter(col("event_type") == "purchase") \
        .groupBy("user_id", "brand") \
        .agg(count("*").alias("t"))
    
    window_brand = Window.partitionBy("user_id").orderBy(desc("t"))
    df_preferred_brand = df_brand.withColumn("rank", rank().over(window_brand)) \
        .filter(col("rank") <= 3)\
        .groupBy("user_id").agg(collect_list("brand").alias("preferred_brands"))

# muc do trung thanh
    df_churn = df.groupBy("user_id").agg(
            min(col("event_time")).alias("first_activity_date"),
            max(when(col("event_type") == "purchase", col("event_time"))).alias("last_purchase_date"),
            max(when(col("event_type").isin("view","cart"), col("event_time"))).alias("last_active_day")
        )
    df_churn = df_churn.withColumn("day_since_last_purchase", datediff(col("last_purchase_date"),col("first_activity_date")))\
                    .withColumn("day_since_first_activity", datediff(col("last_active_day"),col("first_activity_date")))

    df_churn = df_churn.withColumn("chunk_risk",
                                    when(col("day_since_last_purchase") > 30, "High")
                                    .when((col("day_since_last_purchase") > 15) & (col("day_since_first_activity") < 30), "Potential")
                                    .when(col("day_since_last_purchase" )> 15, "Medium")
                                    .otherwise("Low")
                                    )
    df_churn = df_churn.select("user_id","chunk_risk")
    
    df_profile = df_profile.join(df_preferred_category, "user_id", "left")\
                            .join(df_preferred_brand,"user_id","left")\
                            .join(df_churn,"user_id","left")


#san pham goi y

    return df_profile

#Hiệu suất sản phẩm và hành vi mua hàng của từng sản phẩm
def compute_product_performance(df):
    df_perf = df.groupBy("product_id", "category_id").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "cart", True)).alias("total_carts"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        round(sum(when(col("event_type") == "purchase", col("price")).otherwise(0)), 2).alias("total_revenue")
    )
    df_perf = df_perf.withColumn("conversion_rate", round(col("total_purchases")/col("total_views"),2)) \
                .withColumn("cart_abandon_rate",
                            when(col("total_carts") > 0, round((col("total_carts")-col("total_purchases"))/col("total_carts"),2))
                            .otherwise(0))



def transform(year, month, day):
    spark = SparkSession.builder \
        .appName("Transformation") \
        .master("spark://spark-master:7077") \
        .config("spark.jars","/opt/airflow/code/postgresql-42.2.5.jar")\
        .getOrCreate()
    
    df = spark.read.parquet(f"hdfs://namenode:9000/tmp/year={year}/month={month}/day={day:02d}")
    df.show()
    df.printSchema()
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-warehouse:5432/datawarehouse") \
        .option("dbtable", "fact_transactions") \
        .option("user", "datawarehouse") \
        .option("password", "datawarehouse") \
        .option("driver", "org.postgresql.Driver")\
        .mode("append")\
        .save()
    df_profile = compute_static_customer_profile(df)

    df_pert = compute_product_performance(df)

if __name__ == "__main__":
    transform(2019,10,1)


