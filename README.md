# DATN
**Customer Data Platform**

***1. Tổng quan hệ thống***
![123123123](https://github.com/user-attachments/assets/cf557809-36f2-4f25-bde7-f4504181586f)

**1.1. Nguồn dữ liệu**
(https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
   - Xây dựng Flask Server trả Data qua GET EndPoint
     
**1.2. Thu thập dữ liệu**
   - Dữ liệu được Producer gọi và đẩy vào hàng đợi Kafka real-time từng bản ghi một
   - SparkStreaming đọc từ luồng Kafka, chuẩn hóa cấu trúc dữ liệu và ghi vào HDFS
     
**1.3. Xử lý dữ liệu**
   - Dữ thô sẽ được xử lý, làm sạch, biến đổi thành các chỉ số của hồ sơ khách hàng
   - Tính toán chỉ số tổng hợp của từng khách hàng theo từng ngày => tạo dashboard
     
**1.4. Lập lịch giám sát**
    - Quá trình ETL sẽ được lập lịch tự động và quản lý thông qua Apache Airflow
    
**1.5. Triển khai**
    - Docker: triển khai Flask, Airflow, Cụm Kafka, Cụm Spark, Cụm Hadoop, Postgre, Mongodb
    
**1.6. Nền tảng phân khúc**
    - Phân khúc dựa trên luật: chỉ số + logic AND/OR
    - Ứng dụng Web: quản lý hồ sơ, quản lý phân khúc
    
***2. Kết quả***

**Triển khai cụm Kafka**
   <img width="968" height="250" alt="image" src="https://github.com/user-attachments/assets/c986ad4f-b069-4bf1-9c50-2dffef2fbdf7" />
   
**Triển khai cụm Spark**
     <img width="1475" height="344" alt="image" src="https://github.com/user-attachments/assets/a2c16c2a-f9a0-40cd-a867-d0a741799578" />
     
**Triển khai cụm Hadoop**
     <img width="679" height="438" alt="image" src="https://github.com/user-attachments/assets/65ff895e-ed5c-42d9-b049-fc185226fa4e" />
     
**DAG Airflow**
     <img width="1354" height="533" alt="image" src="https://github.com/user-attachments/assets/51b79599-c141-41ee-9088-1596ef29cb8c" />
     
**Quản lý hồ sơ khách hàng**

     *Tổng quan hồ sơ*
   <img width="866" height="525" alt="image" src="https://github.com/user-attachments/assets/ee584aaf-a78d-4fde-b96e-c79d64882697" />
     
     *Chi tiết hồ sơ*
   <img width="688" height="545" alt="image" src="https://github.com/user-attachments/assets/21b28c51-ed5d-46f8-a658-c3f5d71473ef" />

     *Trực quan biểu đồ* 
   <img width="897" height="603" alt="image" src="https://github.com/user-attachments/assets/857940e7-2e72-4631-9d0d-eb6246ba2bcd" />

**Quản lý phân khúc**

   <img width="1019" height="354" alt="image" src="https://github.com/user-attachments/assets/3b1daa46-aac3-4c53-8669-ff32e468e665" />
   
   <img width="829" height="523" alt="image" src="https://github.com/user-attachments/assets/cf5b44a2-4281-452a-8289-ee0ebffd7cec" />








