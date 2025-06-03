from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from pyspark.ml.recommendation import ALS

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

if __name__ == "__main__":
    reco_product()