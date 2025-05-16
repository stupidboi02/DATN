from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.ml.recommendation import ALSModel
from pymongo import MongoClient
import pymongo
spark = SparkSession.builder \
        .appName("hihi") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# df_1 = spark.read.parquet("hdfs://namenode:9000/tmp/year=2019/month=10/day=02")

# df_2 = spark.read.parquet("hdfs://namenode:9000/staging/year=2019/month=10/day=02")
# df_2.where(col("user_id")=="525454715").select("purchase_history").show(truncate=False)
def compute_recommendations(df_logs,k):
       model = ALSModel.load("hdfs://namenode:9000/models/als")
       print("load xong model")
       #user da train
       trained_user = model.userFactors.select(col("id").alias("user_id")).distinct()
       #user log hang ngay
       logs_user = df_logs.select(col("user_id")).distinct()
       #new user
       new_users = logs_user.join(trained_user, on="user_id", how="left_anti")

       top_k_products_df = df_logs.dropna(subset=["product_id"]).groupBy("product_id")\
                                .agg(count("*").alias("count"))\
                                .orderBy(col("count").desc())\
                                .limit(k)
       #list product
       top_k_product = top_k_products_df.agg(collect_list("product_id").alias("recommendations"))\
                                        .collect()[0]["recommendations"]
       
       # Tạo DataFrame chứa list sản phẩm phổ biến
       top_k_df = spark.createDataFrame([(top_k_product,)], ["recommendations"])
       
       new_user_result = new_users.crossJoin(top_k_df)

       recommendation = model.recommendForAllUsers(k)
       recommendation = recommendation.select("user_id",explode("recommendations").alias("rec"))\
                    .select("user_id","rec.product_id")
       
       his_user_result  = recommendation.groupBy("user_id").agg(collect_list("product_id").alias("recommendations"))
       result = his_user_result.union(new_user_result)
       return result 
def transform_row_to_dict(row):
       return{
              "user_id":row.user_id,
              "recommendations":row.recommendations
       }
if __name__ == "__main__":
        df = spark.read.parquet("hdfs://namenode:9000/tmp/year=2019/month=11/day=01")
        res = compute_recommendations(df,5)
        res.show(20)
        client = MongoClient("mongodb://admin:admin@mongo:27017/")
        db = client["admin"]
        collection = db["user_recommend"]
        data_to_insert = [transform_row_to_dict(row) for row in res.collect()]
        collection.create_index([("user_id", pymongo.ASCENDING)], unique=True)
        sorted(list(collection.index_information()))
        collection.insert_many(data_to_insert)

# df_3 = spark.read.parquet("hdfs://namenode:9000/staging/year=2019/month=10/day=03")

# df_1.where(col("last_purchase_date").isNotNull()).show()
# print(df_1.dropDuplicates().count())
# print(df_1.select("user_id").distinct().count())
# print(df_1.count())


