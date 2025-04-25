from pyspark.sql import SparkSession
from pyspark.sql.functions import*

spark = SparkSession.builder \
        .appName("hihi") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df_1 = spark.read.parquet("hdfs://namenode:9000/staging/year=2019/month=10/day=02")

# df_2 = spark.read.parquet("hdfs://namenode:9000/staging/year=2019/month=10/day=02")

# df_3 = spark.read.parquet("hdfs://namenode:9000/staging/year=2019/month=10/day=03")

# df_1.where(col("last_purchase_date").isNotNull()).show()
df_1.where(col("user_id")=="513282532").show(truncate=False)
# df_2.filter(col("user_id")=="551803602").show()

# df_3.filter(col("user_id")=="551803602").show()
