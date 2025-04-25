from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_to_dwh(table,path):
    df = spark.read.parquet(path)
    target_date = df.select("snapshot_date").distinct().orderBy("snapshot_date", ascending=False).limit(1).collect()[0][0]
    df = df.filter(col("snapshot_date") == target_date)
    df.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-warehouse:5432/datawarehouse") \
        .option("dbtable", f"{table}") \
        .option("user", "datawarehouse") \
        .option("password", "datawarehouse") \
        .option("driver", "org.postgresql.Driver")\
        .mode("overwrite")\
        .save()
    
if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("LoadToDWH")\
        .master("spark://spark-master:7077")\
        .config("spark.jars","/opt/airflow/code/postgresql-42.2.5.jar")\
        .getOrCreate()
    load_to_dwh("customer_profiles", "hdfs://namenode:9000/staging/customer_profile")
    load_to_dwh("preferred_categories", "hdfs://namenode:9000/staging/customer_prefer_category")
    load_to_dwh("preferred_brands", "hdfs://namenode:9000/staging/customer_prefer_brand")
    load_to_dwh("customer_churn", "hdfs://namenode:9000/staging/customer_churn_risk")