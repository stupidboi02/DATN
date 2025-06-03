from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType, DoubleType
from datetime import datetime
from pyspark.sql.functions import*
from datetime import datetime
import sys

def load_to_mongo(df_event,df_support):
    user_profile_df = (df_event.alias("event")).join((df_support).alias("support"),on="user_id",how="outer")
    final_profile = user_profile_df.select(
            col("user_id"),
            # Coalesce các trường từ event-based profile (thường là nguồn chính)
            coalesce(col("event.first_visit_timestamp"), lit(None).cast(TimestampType())).alias("first_visit_timestamp"),
            coalesce(col("event.last_visit_timestamp"), lit(None).cast(TimestampType())).alias("last_visit_timestamp"),
            coalesce(col("event.last_purchase_date"), lit(None).cast(TimestampType())).alias("last_purchase_date"),
            coalesce(col("event.last_active_date"), lit(None).cast(TimestampType())).alias("last_active_date"),
            coalesce(col("event.total_visits"), lit(0)).alias("total_visits"),
            coalesce(col("event.purchase_history"), array()).alias("purchase_history"),
            coalesce(col("event.category_preferences"), create_map()).alias("category_preferences"),
            coalesce(col("event.brand_preferences"), create_map()).alias("brand_preferences"),
            coalesce(col("event.churn_risk"),lit(None)).alias("churn_risk"),
            coalesce(col("event.total_items_purchased"), lit(0)).alias("total_items_purchased"),
            coalesce(col("event.total_spend"), lit(0.0)).alias("total_spend"),
            coalesce(col("event.segments"), lit("General Audience")).alias("segments"),
            coalesce(col("event.update_day"), lit(snapshot_date).cast(DateType())).alias("update_day"), # Cập nhật ngày snapshot
            
            # Coalesce các trường từ support-based profile
            coalesce(col("support.total_support_interactions"), lit(0)).alias("total_support_interactions"),
            coalesce(col("support.total_calls"), lit(0)).alias("total_calls"),
            coalesce(col("support.total_chats"), lit(0)).alias("total_chats"),
            coalesce(col("support.total_tickets"), lit(0)).alias("total_tickets"),
            coalesce(col("support.last_support_interaction_time"), lit(None).cast(TimestampType())).alias("last_support_interaction_time"),
            coalesce(col("support.avg_satisfaction_score"), lit(None).cast(DoubleType())).alias("avg_satisfaction_score"),
            coalesce(col("support.most_frequent_issue_category"), create_map()).alias("most_frequent_issue_category"),
            coalesce(col("support.support_prone_flag"), lit(False)).alias("support_prone_flag"),
        
            )
    final_profile.write.format("mongodb") \
                    .mode("overwrite") \
                    .option("collection", "user_profile") \
                    .save()
if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("LoadToDWH")\
        .master("spark://spark-master:7077")\
        .config("spark.mongodb.read.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_profile") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_profile") \
        .getOrCreate()
    
    snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    year, month, day = snapshot_date.year, str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)

    df_event = spark.read.parquet(f"hdfs://namenode:9000/staging/event/year={year}/month={month}/day={day}")
    df_support = spark.read.parquet(f"hdfs://namenode:9000/staging/support/year={year}/month={month}/day={day}")

    load_to_mongo(df_event,df_support)

