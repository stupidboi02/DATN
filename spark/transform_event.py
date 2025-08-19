from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.sql.types import *
from datetime import datetime, timedelta
import sys

user_profile_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("first_visit_timestamp", TimestampType(), True),
    StructField("last_visit_timestamp", TimestampType(), True),
    StructField("last_purchase_date", TimestampType(), True),
    StructField("last_active_date", TimestampType(), True),
    StructField("total_visits", IntegerType(), True),
    StructField("purchase_history", ArrayType(StructType([
        StructField("order_id", StringType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("total_amount", FloatType(), False),
        StructField("items", ArrayType(StructType([
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("price", FloatType(), False)
        ])), False)
    ])), True),
    StructField("category_preferences", MapType(StringType(), FloatType()), True),
    StructField("brand_preferences", MapType(StringType(), FloatType()), True),
    StructField("total_items_purchased", IntegerType(), True),
    StructField("total_spend", DoubleType(), True),
    StructField("segments", StringType(), True),
    StructField("churn_risk",StringType(), True),
    StructField("update_day", DateType(), True)
])

def compute_timestamp(df_logs):
    return df_logs.groupBy("user_id").agg(
           min("event_time").alias("first_visit_timestamp"),
           max("event_time").alias("last_visit_timestamp"),
           max(when(col("event_type")=="purchase",col("event_time"))).alias("last_purchase_date"),
           max(when(col("event_type").isin("view","cart"), col("event_time"))).alias("last_active_date"),
           countDistinct("user_session").alias("total_visits"))\
            .select("user_id", "first_visit_timestamp","last_visit_timestamp","last_purchase_date","last_active_date","total_visits")
 
def compute_purchase_history(df_logs):
    product_agg = df_logs.filter(col("event_type") == "purchase") \
        .groupBy("user_id", "user_session", "product_id") \
        .agg(
            count("*").alias("quantity"),
            round(first("price")).alias("price"),
            min("event_time").alias("order_timestamp")
        )

    purchase_history_df = product_agg.groupBy("user_id", "user_session").agg(
        round(sum(col("quantity") * col("price")), 2).alias("total_amount"),
        first("order_timestamp").alias("order_timestamp"),
        collect_list(
            struct(col("product_id"), col("quantity"), col("price"))
        ).alias("items")) \
        .withColumnRenamed("user_session", "order_id")

    daily_purchase_history = purchase_history_df.groupBy("user_id").agg(
        collect_list(
            struct(col("order_id"), col("order_timestamp"), col("total_amount"), col("items"))
        ).alias("purchase_history"))
    return daily_purchase_history


def compute_category_preferences(df_logs):
        event_weights={"view": 0.1, "cart": 0.2, "purchase":0.3}
        scored_events = df_logs.withColumn("score",
                    when(col("event_type") == "view", lit(event_weights.get("view", 0)))
                    .when(col("event_type") == "purchase", lit(event_weights.get("purchase", 0)))
                    .when(col("event_type") == "cart", lit(event_weights.get("cart", 0)))
                    .otherwise(lit(0.0)))\
                                .withColumn("category_code", coalesce("category_code", lit("unknow")))
        
        category_preferences = scored_events.groupBy("user_id","category_code").agg(sum("score").alias("total_score"))\
        .withColumn("total_score",round(col("total_score"),2))\
        .groupBy("user_id").agg(map_from_entries((collect_list(struct("category_code","total_score")))).alias("category_preferences"))

        return category_preferences

def compute_brand_preferences(df_logs):
        event_weights={"view": 0.1, "cart": 0.2, "purchase":0.3}
        scored_events = df_logs.withColumn("score",
                    when(col("event_type") == "view", lit(event_weights.get("view", 0)))
                    .when(col("event_type") == "purchase", lit(event_weights.get("purchase", 0)))
                    .when(col("event_type") == "cart", lit(event_weights.get("cart", 0)))
                    .otherwise(lit(0.0)))\
                                .withColumn("brand", coalesce("brand", lit("unknow")))
        
        brand_preferences = scored_events.groupBy("user_id","brand").agg(sum("score").alias("total_score"))\
        .withColumn("total_score",round(col("total_score"),2))\
        .groupBy("user_id").agg(map_from_entries((collect_list(struct("brand","total_score")))).alias("brand_preferences"))
        return brand_preferences

def compute_total_items_purchased(df_logs):
       df = df_logs.filter(col("event_type")=="purchase")\
                    .groupBy("user_id")\
                    .agg(count("*").alias("total_items_purchased"),
                         round(sum("price")).alias("total_spend"))
       return df

def transform(snapshot_date, df_logs):

    prev_snapshot = snapshot_date - timedelta(1)
    y, m, d = prev_snapshot.year, str(prev_snapshot.month).zfill(2), str(prev_snapshot.day).zfill(2)
    user_profile_pre = f"hdfs://namenode:9000/staging/event/year={y}/month={m}/day={d}"

    timestamp_df = compute_timestamp(df_logs)
    purchase_history = compute_purchase_history(df_logs)
    category_preferences = compute_category_preferences(df_logs)
    brand_preferences = compute_brand_preferences(df_logs)
    total_item_purchased = compute_total_items_purchased(df_logs)

    daily_profile = timestamp_df\
                    .join(purchase_history,"user_id","left")\
                    .join(category_preferences,"user_id","left")\
                    .join(brand_preferences,"user_id","left")\
                    .join(total_item_purchased,"user_id","left")\
                    .withColumn("update_day", lit(snapshot_date))
    
    daily_profile_rename = daily_profile.selectExpr(
            "user_id as user_id",
            "first_visit_timestamp as first_visit_today",
            "last_visit_timestamp as last_visit_today",
            "last_purchase_date as last_purchase_today",
            "last_active_date as last_active_today",
            "total_visits as total_visits_today",
            "purchase_history as purchase_history_today",
            "category_preferences as category_preferences_today",
            "brand_preferences as brand_preferences_today",
            "total_items_purchased as total_items_purchased_today",
            "total_spend as total_spend_today",
            "update_day as update_day_today"
        )
    try:
        df_profile = spark.read.parquet(user_profile_pre)
    except:
    #neu la ngay dau tien 
        df_profile = spark.createDataFrame([],schema=user_profile_schema)
        first_day_profile = daily_profile.withColumn("segments",when((col("total_spend") > 500000) & (col("total_visits") > 100) & (col("total_items_purchased") > 5),"VIP")
                                                            .when((col("total_spend") > 100000),"High Value")
                                                            .when((col("last_purchase_date") >= date_sub(col("update_day"), 15)) & (col("total_items_purchased") > 0),"Recent Active Buyers")
                                                            .otherwise("General Audience"))\
                                        .withColumn("day_since_last_purchase",date_diff("update_day","last_purchase_date"))\
                                        .withColumn("day_since_last_active",date_diff("update_day","last_active_date"))\
                                        .withColumn("churn_risk", when((col("day_since_last_purchase") >= 30) & (col("day_since_last_active") >= 30), "Very High")
                                                                    .when((col("day_since_last_purchase") >= 20) | (col("day_since_last_active") >= 20),"High")
                                                                    .when((col("day_since_last_purchase") >= 10) & (col("day_since_last_active") < 10), "Normal")
                                                                    .otherwise("Low")
                                    ).drop("day_since_last_purchase","day_since_last_active")
        first_day_profile.write.mode("overwrite").parquet(f"hdfs://namenode:9000/staging/event/year={year}/month={month}/day={day}")

    #neu khong phai ngay dau
    if df_profile.count() > 0:
        update_profile = df_profile.join(daily_profile_rename,"user_id","outer")

    # logic tinh lai brand va cate kieu MAP
        today_exploded = update_profile.select("user_id",explode("category_preferences_today").alias("category", "score"))
        his_exploded = update_profile.select("user_id",explode("category_preferences").alias("category", "score"))
        cate_merge = today_exploded.union(his_exploded)
        category_prefs = (
            cate_merge.groupBy("user_id", "category").agg(round((sum("score")/count("*")),2).alias("total_score"))
                            .groupBy("user_id")
                            .agg(map_from_entries(collect_list(struct("category", "total_score"))).alias("category_preferences"))
                )
        
        today_exploded = update_profile.select("user_id",explode("brand_preferences_today").alias("brand", "score"))
        his_exploded = update_profile.select("user_id",explode("brand_preferences").alias("brand", "score"))
        brand_merge = today_exploded.union(his_exploded)
        brand_prefs = (
            brand_merge.groupBy("user_id", "brand").agg(round((sum("score")/count("*")),2).alias("total_score"))
                            .groupBy("user_id")
                            .agg(map_from_entries(collect_list(struct("brand", "total_score"))).alias("brand_preferences"))
                )

        result = update_profile \
                .withColumn("first_visit_timestamp", least("first_visit_today", "first_visit_timestamp")) \
                .withColumn("last_visit_timestamp", greatest("last_visit_today", "last_visit_timestamp")) \
                .withColumn("last_purchase_date", greatest("last_purchase_today", "last_purchase_date")) \
                .withColumn("last_active_date", greatest("last_active_today", "last_active_date")) \
                .withColumn("total_visits", coalesce(col("total_visits_today"), lit(0)) + coalesce(col("total_visits"), lit(0))) \
                .withColumn("purchase_history", array_union(
                    coalesce(col("purchase_history_today"), lit([])),
                    coalesce(col("purchase_history"), lit([]))))\
                .withColumn("total_items_purchased",coalesce(col("total_items_purchased_today"), lit(0)) + coalesce(col("total_items_purchased"), lit(0)))\
                .withColumn("total_spend",coalesce(col("total_spend_today"), lit(0)) + coalesce(col("total_spend"), lit(0)))\
                .withColumn("update_day", lit(snapshot_date))\
                .drop("category_preferences","brand_preferences","category_preferences_today","brand_preferences_today",
                        "first_visit_today","last_visit_today","last_purchase_today","last_active_today","update_day_today",
                        "total_visits_today","purchase_history_today","total_items_purchased_today","total_spend_today")\
                .join(category_prefs, "user_id", "left")\
                .join(brand_prefs, "user_id", "left")\
        #sau khi update profile
        result = result.withColumn("segments", 
                        when((col("total_spend") > 100000) & (col("total_visits") > 100) & (col("total_items_purchased") > 5),"VIP")
                        .when((col("total_spend") > 50000),"High Value")
                        .when((col("last_purchase_date") >= date_sub(col("update_day"), 15)) & (col("total_items_purchased") > 0),"Recent Active Buyers")
                        .otherwise("General Audience"))\
                    .withColumn("day_since_last_purchase",date_diff("update_day","last_purchase_date"))\
                    .withColumn("day_since_last_active",date_diff("update_day","last_active_date"))\
                    .withColumn("churn_risk", when((col("day_since_last_purchase") >= 30) & (col("day_since_last_active") >= 30), "Very High")
                                            .when((col("day_since_last_purchase") >= 20) | (col("day_since_last_active") >= 20),"High")
                                            .when((col("day_since_last_purchase") >= 10) & (col("day_since_last_active") < 10), "Normal")
                                            .otherwise("Low")
                                ).drop("day_since_last_purchase","day_since_last_active")

        result.write.mode("overwrite").parquet(f"hdfs://namenode:9000/staging/event/year={year}/month={month}/day={day}")
if __name__ == "__main__":
        spark = SparkSession.builder \
        .appName("TransformEvent") \
        .master("spark://spark-master:7077")\
        .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()

        year, month, day = snapshot_date.year, str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)

        logs_day_path = f"hdfs://namenode:9000/raw_event/year={year}/month={month}/day={day}"

        df_logs = spark.read.parquet(logs_day_path)

        transform(snapshot_date,df_logs)
        # for x in range(1,32):
        #     snapshot_date = datetime.strptime(f"2019-11-{x}", "%Y-%m-%d").date()

        #     year, month, day = snapshot_date.year, str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)

        #     logs_day_path = f"hdfs://namenode:9000/raw_event/year={year}/month={month}/day={day}"

        #     df_logs = spark.read.parquet(logs_day_path)

        #     transform(snapshot_date,df_logs)


               


