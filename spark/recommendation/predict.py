from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.ml.recommendation import ALSModel

spark = SparkSession.builder \
       .appName("predict") \
       .master("spark://spark-master:7077") \
       .config("spark.mongodb.read.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_recommend") \
       .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_recommend") \
       .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def compute_recommendations(df_logs,k):
       model = ALSModel.load("hdfs://namenode:9000/models/als")
       print("load xong model")
       #user da train
       trained_user = model.userFactors.select(col("id").alias("user_id")).distinct()
       #user log hang ngay
       logs_user = df_logs.select(col("user_id")).distinct()
       #new user
       new_users = logs_user.join(trained_user, on="user_id", how="left_anti")
       # Chọn ra top k sản phẩm phổ biến của ngày hôm đó
       top_k_products_df = df_logs.dropna(subset=["product_id"]).groupBy("product_id")\
                                .agg(count("*").alias("count"))\
                                .orderBy(col("count").desc())\
                                .limit(k)
       # Chuyển về mảng sản phẩm
       top_k_product = top_k_products_df.agg(collect_list("product_id").alias("recommendations"))\
                                        .collect()[0]["recommendations"]
       
       # Tạo DataFrame chứa list sản phẩm phổ biến
       top_k_df = spark.createDataFrame([(top_k_product,)], ["recommendations"])
       # Gán top k sản phẩm cho mỗi user mới
       new_user_result = new_users.crossJoin(top_k_df)

       recommendation = model.recommendForAllUsers(k)
       recommendation = recommendation.select("user_id",explode("recommendations").alias("rec"))\
                    .select("user_id",col("rec.product_id").alias("product_id"))
       
       his_user_result  = recommendation.groupBy("user_id").agg(collect_list("product_id").alias("recommendations"))
       result = his_user_result.union(new_user_result)
       return result 

if __name__ == "__main__":
       df_logs = spark.read.parquet(f"hdfs://namenode:9000/raw_event/year=2019/month=11/day=01")
       df = compute_recommendations(df_logs, 5)
       df.write.format("mongodb") \
        .mode("overwrite") \
        .option("collection", "user_recommend") \
        .save()

