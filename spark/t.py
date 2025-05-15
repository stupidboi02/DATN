from pyspark.sql import SparkSession
from pyspark.sql.functions import*

#Hiệu suất sản phẩm và hành vi mua hàng của từng sản phẩm
def product_views(df):
    df_product_views = df.where(col("event_type") == "view")
    return

# if __name__ == "__main__":
#     return 12 