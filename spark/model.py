from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.ml.recommendation import ALS
from datetime import datetime, timedelta

def reco_product():
    spark = SparkSession.builder.appName("RECO ALS").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    raw_df = spark.read.parquet("hdfs://namenode:9000/tmp/year=2019/month=10/*")
    df = raw_df.dropna(subset=["user_id","product_id","event_type"])
    df = df.withColumn("ratings",
                       when((col("event_type")=="view"),1.0)
                       .when((col("event_type")=="cart"),3.0)
                       .when((col("event_type")=="purchase"),5.0)
                       .otherwise(lit(0.0))
                       )
    # df.show()
    data = df.groupBy("user_id","product_id").agg(sum(col("ratings")).alias("ratings"))\
            .select(
                col("user_id").cast("integer"),
                col("product_id").cast("integer"),
                col("ratings")
            ).na.drop()
    
    if data.count() == 0:
        return
    
#     # Train ALS Model
    als = ALS(
        userCol="user_id",
        itemCol="product_id",
        ratingCol="ratings",
        coldStartStrategy="drop",
        nonnegative=True,
        implicitPrefs=False,
        maxIter=10,
        rank=10,
        regParam=0.1
    )
    model = als.fit(data)
    
    model.write().overwrite().save("hdfs://namenode:9000/models/als")

#     # Dự đoán sản phẩm cho tất cả người dùng
#     user_recs = model.recommendForAllUsers(num_products)

#     result = user_recs.select(
#         col("user_id"),
#         expr("transform(recommendations, x -> x.product_id) as recommended_products")
#     ).withColumn("recommend_date", expr(f"'{date}'"))

#     result.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://data-warehouse:5432/datawarehouse") \
#         .option("dbtable", "recommend") \
#         .option("user", "datawarehouse") \
#         .option("password", "datawarehouse") \
#         .option("driver", "org.postgresql.Driver")\
#         .mode("overwrite")\
#         .save()
#     spark.stop()

if __name__ == "__main__":
    reco_product()