from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json,requests, time, sys
from datetime import datetime
import pandas as pd

config = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
    "queue.buffering.max.kbytes": 512000,
    "queue.buffering.max.messages": 1000000,
    "batch.num.messages": 5000,
          }
admin_client = AdminClient({'bootstrap.servers': 'kafka-0:9092,kafka-1:9092,kafka-2:9092'})

new_topic_stop = [
    NewTopic(
    topic="logs_data_11",
    num_partitions=2,
    replication_factor=1
    ),
    NewTopic(
    topic="logs_data_10",
    num_partitions=2,
    replication_factor=1
    ),
    NewTopic(
    topic="customer_support_logs",
    num_partitions=2,
    replication_factor=1
    )
]
# tạo topic
try:
    admin_client.create_topics(new_topic_stop)
except Exception as e:
    print(f"Error creating topics: {e}")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer(config)
url = "http://flask-service:5000/get-data"

def flush_event_logs(year,month,day):
    print(f"started for {year}-{month}-{day:02d}")
    limit = 10000
    offset = 0
    params = {"year":year,"month":month,"day":f"{day:02d}","offset":offset,"limit":limit}
    topic = f"logs_data_{month}"
    while True:
        try:
            start_time = time.time()
            print("Calling API with params:", params)
            res = requests.get(url=url, params=params)
            end_time = time.time()
            print(f"Thời gian gọi API: {end_time - start_time:.2f} giây")
            data = res.json()
            # print(data)
            if data['state'] == 'error':
                break

            if data['state'] == 'success' or data['state'] == 'complete':
                key = str(year) + '_' + str(month) + '_' +str(day) +'_' + str(params['offset'])
                print(f"{key}-{params['offset']+limit}")
                for record in data['data']:
                    value = json.dumps(record)
                    producer.produce(topic, value, key, callback=delivery_report)
                    producer.poll(0)        

                if data['state'] == 'complete':
                    print("event logs stream complete")
                    time.sleep(3)
                    break

                start_time1 = time.time()
                producer.flush()
                end_time1 = time.time()
                print(f"Thời gian flush Kafka-{month}-{day:02d}: {end_time1 - start_time1:.2f} giây")

                params['offset'] = params['offset'] + limit

        except Exception as e:
            print(e)
            break
    producer.flush()

def flush_customer_support_logs(year,month,day):
    print(f"started for {year}-{month}-{day:02d}")
    df = pd.read_csv("/app/customer_support_logs.csv")
    df['event_time'] = pd.to_datetime(df['event_time'])

    # Chuyển đổi event_time về dạng chỉ ngàyy
    df['event_date'] = df['event_time'].dt.date
    # Đặt event_date làm index để tối ưu tốc độ
    df.set_index('event_date', inplace=True)
    target_date = datetime(year, month, day).date()
    try:
        if df.index.name == 'event_date':
            new_df = df.loc[target_date].sort_values(by='event_time').reset_index(drop=True)
    except Exception as e:
        print(f"Ngày không hợp lệ {year}-{month}-{day:02d}: {e}")
        return

    #produce to kafka
    topic = "customer_support_logs"
    batch_size = 1000
    batch = []
    count = 0
    for i in range(len(new_df)):
        row = new_df.iloc[i]
        value=json.dumps(row.to_dict(), default=str)
        batch.append(value)
        if len(batch) >= batch_size:
            for msg in batch:
                #Gửi không đồng bộ => tối ưu tốc độ
                key = str(year) + '_' + str(month) + '_' +str(day) +'_' + str(batch_size)
                print(msg)
                producer.produce(topic, msg, key,callback = delivery_report)
            count += len(batch)
            print(f"Gửi {len(batch)} bản ghi tới {topic}. Tổng: {count} bản")
            batch = []

    print("support logs stream complete")
    producer.poll(0)
    producer.flush()

if __name__ == "__main__":
    # date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    # year, month, day = date.year, date.month, date.day
    # flush_event_logs(year,month,day)
    for x in range(1,32):
        # flush_event_logs(2019,10,x)
        flush_customer_support_logs(2019,10,x)


