from datetime import datetime, timedelta
import numpy as np
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def generate_customer_support_logs(unique_user_ids, num_logs=2000000):
    data = []

    # Chọn ra 70% user_id để có tương tác hỗ trợ
    num_users_for_support = int(len(unique_user_ids) * 0.7)
    selected_users = np.random.choice(unique_user_ids, size=num_users_for_support, replace=False)
    selected_users = selected_users.tolist()
    # Giả lập thời gian từ 01/01/2025 đến 25/05/2025
    start_date = datetime(2019, 10, 1)
    end_date = datetime(2019, 11, 30)

    for _ in range(num_logs):
        user_id = random.choice(selected_users)

        # Sinh ngẫu nhiên một thời điểm trong khoảng thời gian trên
        time_delta = end_date - start_date
        random_seconds = random.randint(0, int(time_delta.total_seconds()))
        event_time = start_date + timedelta(seconds=random_seconds)

        # Loại hình tương tác: gọi điện, chat, hoặc gửi ticket
        interaction_type = random.choices(['call', 'chat', 'ticket'], weights=[0.5, 0.4, 0.1])[0]

        # Loại vấn đề gặp phải
        issue_category = random.choice([
            'product_inquiry', 'delivery_issue', 'refund_request', 
            'technical_support', 'billing_question', 'other'
        ])

        # Trạng thái xử lý
        resolution_status = random.choices(['resolved', 'unresolved'], weights=[0.8, 0.2])[0]

        # Điểm hài lòng nếu vấn đề đã được giải quyết
        satisfaction_score = random.randint(1, 5) if resolution_status == 'resolved' else None

        # Lưu lại thông tin 1 log hỗ trợ
        data.append({
            'event_time': event_time,
            'user_id': user_id,
            'interaction_type': interaction_type,
            'issue_category': issue_category,
            'resolution_status': resolution_status,
            'satisfaction_score': satisfaction_score
        })

    return data
if __name__ == "__main__":
    spark = SparkSession.builder.appName("SupportLogs").getOrCreate()
    df = spark.read.parquet("hdfs://namenode:9000/raw_event/year=2019/month=10/")
    # Lấy danh sách user_id duy nhất từ DataFrame
    unique_user_ids = df.select("user_id").distinct().rdd.flatMap(lambda x: x).collect()
    # Tạo dữ liệu log
    logs = generate_customer_support_logs(unique_user_ids, num_logs=2000000)

    # Định nghĩa schema để đảm bảo đúng kiểu dữ liệu
    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("interaction_type", StringType(), True),
        StructField("issue_category", StringType(), True),
        StructField("resolution_status", StringType(), True),
        StructField("satisfaction_score", IntegerType(), True)
    ])

    df_support_logs = spark.createDataFrame(logs, schema=schema)

    df_support_logs.show(5, truncate=False)

    df_support_logs.coalesce(1).write.csv("file:///tmp/customer_support_logs.csv", header=True, mode='overwrite')
