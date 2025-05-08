from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import*
import sys
from pymongo import MongoClient
import pymongo

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("LoadToDWH")\
        .master("spark://spark-master:7077")\
        .getOrCreate()
        # .config("spark.mongodb.read.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_profile") \
        # .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_profile") \
    
    snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    year, month, day = snapshot_date.year, str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)

    df = spark.read.parquet(f"hdfs://namenode:9000/staging/year={year}/month={month}/day={day}")

    df = [row.asDict() for row in df.collect()]

    client = MongoClient("mongodb://admin:admin@mongo:27017/")
    db = client["admin"]
    collection = db["user_profile"]

    collection.create_index([("user_id", pymongo.ASCENDING)], unique=True)

    sorted(list(collection.index_information()))

    collection.insert_many(df)
