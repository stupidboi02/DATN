from confluent_kafka import Producer
import json
import requests
import threading
import time

config = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
    "queue.buffering.max.kbytes": 512000,
    "queue.buffering.max.messages": 1000000,
    "batch.num.messages": 5000,
          }
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer(config)
url = "http://flask-service:5000/get-data"

def flush(year,month,day):
    print(f"Thread started for {year}-{month}-{day:02d}")
    limit = 20000
    offset = 0
    params = {"year":year,"month":month,"day":f"{day:02d}","offset":offset,"limit":limit}
    topic = f"logs_data_{month}"
    while True:
        try:
            start_time0 = time.time()
            res = requests.get(url=url, params=params)
            end_time0 = time.time()
            print(f"Thời gian gọi API: {end_time0 - start_time0:.2f} giây")
            data = res.json()
            print(data)
            if data['state'] == 'error':
                break

            if data['state'] == 'success' or data['state'] == 'complete':
                key = str(year) + '_' + str(month) + '_' +str(day) +'_' + str(params['offset'])
                print(f"{key}-{params['offset']+limit}")
                for record in data['data']:
                    value = json.dumps(record)
                    producer.produce(topic, value, key)
                    producer.poll(0)        

                if data['state'] == 'complete':
                    print("stream complete")
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

if __name__ == "__main__":
    month = 11
    for day in range(1,32):
        flush(2019,month,day)

    days = 31
    # # number_loop = (days//3) + 1
    # for day in range(0, days, 3):  
    #     if day + 1 <= days:
    #         threading.Thread(target=flush, args=(2019, 10, day + 1)).start()
    #     if day + 2 <= days:
    #         threading.Thread(target=flush, args=(2019, 10, day + 2)).start()
    #     if day + 3 <= days:
    #         threading.Thread(target=flush, args=(2019, 10, day + 3)).start()

    # max_workers = 5  # Chỉ chạy tối đa 5 luồng cùng lúc
    # with ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     for day in range(1, days + 1):
    #         executor.submit(flush, 2019, 10, day)
