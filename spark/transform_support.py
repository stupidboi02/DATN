from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import*
from pyspark.sql.window import Window
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("TransformSupport").getOrCreate()

support_schema= StructType([
            StructField("total_support_interactions", IntegerType(), True),
            StructField("total_calls", IntegerType(), True),
            StructField("total_chats", IntegerType(), True),
            StructField("total_tickets", IntegerType(), True),
            StructField("total_resolved_issues", IntegerType(), True),
            StructField("total_unresolved_issues", IntegerType(), True),
            StructField("avg_satisfaction_score", DoubleType(), True),
            StructField("most_frequent_issue_category", MapType(StringType(), IntegerType()), True),
            StructField("support_prone_flag", BooleanType(), True),
])
def transform_support_data(df):
    df_tsi = df.groupBy("user_id").agg(
        count("*").alias("total_support_interactions"),
        sum(when(col("interaction_type") == "call", 1).otherwise(0)).alias("total_calls"),
        sum(when(col("interaction_type") == "chat", 1).otherwise(0)).alias("total_chats"),
        sum(when(col("interaction_type") == "ticket", 1).otherwise(0)).alias("total_tickets"),
        max(col("event_time")).alias("last_support_interaction_time"),
        sum(when(col("resolution_status") == "resolved", 1).otherwise(0)).alias("total_resolved_issues"),
        sum(when(col("resolution_status") == "unresolved", 1).otherwise(0)).alias("total_unresolved_issues"),
        avg("satisfaction_score").alias("avg_satisfaction_score"),
    ).withColumn("support_prone_flag",
            when(col("total_support_interactions") > 5, True).otherwise(False)
)
    # Loại vấn đề hay gặp
    df_issue = df.groupBy("user_id", "issue_category").agg(count("*").alias("fre"))
    window = Window.partitionBy("user_id").orderBy(col("fre").desc())
    df_issue = df_issue.withColumn("top", row_number().over(window)) \
                       .filter(col("top") <= 3) \
                       .groupBy("user_id").agg(map_from_entries(collect_list(struct(col("issue_category"), col("fre")))).alias("most_frequent_issue_category"))

    result = df_tsi.join(df_issue, on="user_id", how="outer")
    return result

if __name__ == "__main__":
    snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    # bắt đầu từ ngày 1
    start_date = snapshot_date.replace(day=1)
    path = []
    current = start_date
    while current <= snapshot_date:
        year, month, day = current.year, str(current.month).zfill(2), str(current.day).zfill(2)
        tmp = f"hdfs://namenode:9000/raw_support/year={year}/month={month}/day={day}"
        path.append(tmp)
        current += timedelta(days=1)

    df = spark.read.parquet(*path)

    res = transform_support_data(df)
    res.write.mode("overwrite").parquet(f"hdfs://namenode:9000/staging/support/year={year}/month={month}/day={day}")
   
    # for month in [10]:
    #     for day in range(1,32):
    #         snapshot_date=datetime.strptime(f"2019-{month}-{day}", "%Y-%m-%d").date()
    #         start_date = snapshot_date.replace(day=1)
    #         path = []
    #         current = start_date
    #         while current <= snapshot_date:
    #             year, month, day = current.year, str(current.month).zfill(2), str(current.day).zfill(2)
    #             tmp = f"hdfs://namenode:9000/raw_support/year={year}/month={month}/day={day}"
    #             path.append(tmp)
    #             current += timedelta(days=1)

    #         df = spark.read.parquet(*path)

    #         res = transform_support_data(df)
    #         res.write.mode("overwrite").parquet(f"hdfs://namenode:9000/staging/support/year={year}/month={month}/day={day}")
   