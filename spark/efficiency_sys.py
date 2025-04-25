from pyspark.sql import SparkSession
from pyspark.sql.functions import*

#Hiệu suất sản phẩm và hành vi mua hàng của từng sản phẩm
def compute_one_performance(df,colum):
    df_product = df.groupBy(colum).agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "cart", True)).alias("total_carts"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        round(sum(when(col("event_type") == "purchase", col("price")).otherwise(0)), 2).alias("total_revenue")
    )
    df_product = df_product.withColumn("conversion_rate", round(col("total_purchases")/col("total_views"),2)) \
                .withColumn("cart_abandon_rate",
                            when(col("total_carts") > 0, round((col("total_carts")-col("total_purchases"))/col("total_carts"),2))
                            .otherwise(0))

def compute_category_performance(df):
    df_product = df.groupBy("category_id","category_code").agg(
        count(when(col("event_type") == "view", True)).alias("total_views"),
        count(when(col("event_type") == "cart", True)).alias("total_carts"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
        round(sum(when(col("event_type") == "purchase", col("price")).otherwise(0)), 2).alias("total_revenue")
    )
    df_product = df_product.withColumn("conversion_rate", round(col("total_purchases")/col("total_views"),2)) \
                .withColumn("cart_abandon_rate",
                            when(col("total_carts") > 0, round((col("total_carts")-col("total_purchases"))/col("total_carts"),2))
                            .otherwise(0))

# if __name__ == "__main__":
#     return 12 