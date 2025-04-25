from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.ml.recommendation import ALS
from datetime import datetime, timedelta

def reco_product(date, num_days, num_products):
    spark = SparkSession.builder.appName("RECO ALS").getOrCreate()

    target_date = datetime.strptime(date, "%Y-%m-%d")

    dates_to_read = [target_date - timedelta(days=i) for i in range(1, num_days + 1)]

    base_path = "hdfs://namenode:9000/tmp/year=2019"
    paths = []
    for dt in dates_to_read:
        month = str(dt.month).zfill(2)
        day = str(dt.day).zfill(2)
        path = f"{base_path}/month={month}/day={day}"
        paths.append(path)
    df = spark.read.option("basePath", base_path).parquet(*paths)

    df = df.withColumn("ratings", 
                   when(col("event_type") == "view", 1)
                   .when(col("event_type") == "cart", 2)
                   .when(col("event_type") == "purchase", 3)
                   .otherwise(0)
                   )
    df = df.groupBy("user_id","product_id").agg(sum("ratings").alias("ratings"))

    data = df.select(
        col("user_id"),
        col("product_id"),
        col("ratings")
        ).na.drop()
    
    if data.count() == 0:
        return
    
    # Train ALS Model
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

    # Dự đoán sản phẩm cho tất cả người dùng
    user_recs = model.recommendForAllUsers(num_products)

    result = user_recs.select(
        col("user_id"),
        expr("transform(recommendations, x -> x.product_id) as recommended_products")
    ).withColumn("recommend_date", expr(f"'{date}'"))

    result.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-warehouse:5432/datawarehouse") \
        .option("dbtable", "recommend") \
        .option("user", "datawarehouse") \
        .option("password", "datawarehouse") \
        .option("driver", "org.postgresql.Driver")\
        .mode("overwrite")\
        .save()
    spark.stop()

if __name__ == "__main__":
    # 7 days from the calls, 5 product to recommend
    reco_product("2019-11-8",7,5)