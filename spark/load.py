from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import*
import sys
from pymongo import MongoClient
import pymongo
# def handle_none(value, default=[]):
#         """
#         Trả về giá trị hoặc giá trị mặc định nếu giá trị là None.
#         """
#         return value if value is not None else default

# def transform_row_to_dict(row):
#     return {
#         "user_id": handle_none(row.user_id),
#         "first_visit_timestamp": handle_none(row.first_visit_timestamp),
#         "last_visit_timestamp": handle_none(row.last_visit_timestamp),
#         "last_purchase_date": handle_none(row.last_purchase_date),
#         "last_active_date": handle_none(row.last_active_date),
#         "total_visits": handle_none(row.total_visits, 0),
#         "purchase_history": [
#             {
#                 "order_id": ph.order_id,
#                 "order_timestamp": ph.order_timestamp,
#                 "total_amount": ph.total_amount,
#                 "items": [
#                     {"product_id": item.product_id, "quantity": item.quantity, "price": item.price}
#                     for item in ph.items
#                 ]
#             }
#             for ph in handle_none(row.purchase_history)
#         ],
#         "category_preferences":row.category_preferences,
#         "brand_preferences":row.brand_preferences,
#         "total_items_purchased": handle_none(row.total_items_purchased),
#         "total_spend":handle_none(row.total_spend),
#         "update_day":row.update_day,
#         "segments":row.segments,
#         "churn_risk":row.churn_risk
#     }

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("LoadToDWH")\
        .master("spark://spark-master:7077")\
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.read.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_profile") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin@mongo:27017/admin.user_profile") \
        .getOrCreate()
    
    snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    year, month, day = snapshot_date.year, str(snapshot_date.month).zfill(2), str(snapshot_date.day).zfill(2)

    df = spark.read.parquet(f"hdfs://namenode:9000/staging/year={year}/month={month}/day={day}")
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("collection", "user_profile") \
        .save()
    # data_to_insert = [transform_row_to_dict(row) for row in df.collect()]

    # client = MongoClient("mongodb://admin:admin@mongo:27017/")
    # db = client["admin"]
    # collection = db["user_profile"]
    # # Xóa toàn bộ dữ liệu trong collection trước khi ghi dữ liệu mới
    # collection.delete_many({})
    # collection.create_index([("user_id", pymongo.ASCENDING)], unique=True)

    # sorted(list(collection.index_information()))

    # collection.insert_many(data_to_insert)
    # client.close()