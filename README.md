<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Flask-000000?style=flat&logo=flask&logoColor=white"/>
  <img src="https://img.shields.io/badge/React-20232A?style=flat&logo=react&logoColor=61DAFB"/>
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=flat&logo=apachehadoop&logoColor=white"/>
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/MongoDB-47A248?style=flat&logo=mongodb&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white"/>
</p>

<h1 align="center">DATN - Customer Segment Platform</h1>

<p align="center"><strong>Hệ thống xử lý, phân tích dữ liệu khách hàng đa tầng, real-time, trực quan & quản lý phân khúc thông minh</strong></p>

---

## 📑 Mục lục

- [1. Tổng quan hệ thống](#1-tổng-quan-hệ-thống)
  - [1.1. Nguồn dữ liệu](#11-nguồn-dữ-liệu)
  - [1.2. Thu thập dữ liệu](#12-thu-thập-dữ-liệu)
  - [1.3. Xử lý dữ liệu](#13-xử-lý-dữ-liệu)
  - [1.4. Lập lịch giám sát](#14-lập-lịch-giám-sát)
  - [1.5. Triển khai](#15-triển-khai)
  - [1.6. Nền tảng phân khúc](#16-nền-tảng-phân-khúc)
- [2. Kết quả](#2-kết-quả)
- [3. Demo giao diện](#3-demo-giao-diện)
- [4. Kiến trúc tổng thể](#4-kiến-trúc-tổng-thể)
- [5. Công nghệ sử dụng](#5-công-nghệ-sử-dụng)

---

## 1. Tổng quan hệ thống

![Tổng quan hệ thống](https://github.com/user-attachments/assets/cf557809-36f2-4f25-bde7-f4504181586f)

### 1.1. Nguồn dữ liệu

- Dataset: [Ecommerce Behavior Data from Multi Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- Xây dựng Flask Server trả data qua GET Endpoint

### 1.2. Thu thập dữ liệu

- Dữ liệu được Producer gọi và đẩy vào hàng đợi Kafka real-time từng bản ghi một
- Spark Streaming đọc từ luồng Kafka, chuẩn hoá cấu trúc dữ liệu và ghi vào HDFS

### 1.3. Xử lý dữ liệu

- Dữ liệu thô được xử lý, làm sạch, biến đổi thành các chỉ số của hồ sơ khách hàng
- Tính toán chỉ số tổng hợp của từng khách hàng theo từng ngày → tạo dashboard

### 1.4. Lập lịch giám sát

- Quá trình ETL sẽ được lập lịch tự động và quản lý thông qua Apache Airflow

### 1.5. Triển khai

- Docker: Triển khai Flask, Airflow, cụm Kafka, cụm Spark, cụm Hadoop, PostgreSQL, MongoDB

### 1.6. Nền tảng phân khúc

- Phân khúc dựa trên luật: chỉ số + logic AND/OR
- Ứng dụng Web: quản lý hồ sơ, quản lý phân khúc

---

## 2. Kết quả

**Triển khai cụm Kafka**
<br>
<img width="968" height="250" alt="Kafka Cluster" src="https://github.com/user-attachments/assets/c986ad4f-b069-4bf1-9c50-2dffef2fbdf7" />

**Triển khai cụm Spark**
<br>
<img width="1475" height="344" alt="Spark Cluster" src="https://github.com/user-attachments/assets/a2c16c2a-f9a0-40cd-a867-d0a741799578" />

**Triển khai cụm Hadoop**
<br>
<img width="679" height="438" alt="Hadoop Cluster" src="https://github.com/user-attachments/assets/65ff895e-ed5c-42d9-b049-fc185226fa4e" />

**DAG Airflow**
<br>
<img width="1354" height="533" alt="Airflow DAG" src="https://github.com/user-attachments/assets/51b79599-c141-41ee-9088-1596ef29cb8c" />

---

## 3. Demo giao diện

### Quản lý hồ sơ khách hàng

**Tổng quan hồ sơ**
<br>
<img width="866" height="525" alt="Customer Overview" src="https://github.com/user-attachments/assets/ee584aaf-a78d-4fde-b96e-c79d64882697" />

**Chi tiết hồ sơ**
<br>
<img width="688" height="545" alt="Customer Detail" src="https://github.com/user-attachments/assets/21b28c51-ed5d-46f8-a658-c3f5d71473ef" />

**Trực quan biểu đồ**
<br>
<img width="897" height="603" alt="Visualization" src="https://github.com/user-attachments/assets/857940e7-2e72-4631-9d0d-eb6246ba2bcd" />

### Quản lý phân khúc

<img width="1019" height="354" alt="Segmentation 1" src="https://github.com/user-attachments/assets/3b1daa46-aac3-4c53-8669-ff32e468e665" />
<br>
<img width="829" height="523" alt="Segmentation 2" src="https://github.com/user-attachments/assets/cf5b44a2-4281-452a-8289-ee0ebffd7cec" />

---

## 4. Kiến trúc tổng thể

```mermaid
flowchart LR
    subgraph DataSource
        A1[Flask API]
    end
    subgraph Ingestion
        B1[Kafka Producer]
        B2[Kafka Broker]
    end
    subgraph Streaming
        C1[Spark Streaming]
    end
    subgraph Storage
        D1[HDFS]
    end
    subgraph Processing
        E1[Spark Batch]
    end
    subgraph Database
        F1[PostgreSQL]
        F2[MongoDB]
    end
    subgraph Workflow
        G1[Airflow DAG]
    end
    subgraph Presentation
        H1[ReactJS Dashboard]
    end

    A1 --> B1 --> B2 --> C1 --> D1
    G1 -.-> C1
    D1 --> E1
    G1 -.-> E1
    E1 --> F1
    E1 --> F2
    F1 --> H1
    F2 --> H1
```

**Mô tả luồng dữ liệu:**
- Dữ liệu từ Flask API được Producer đẩy lên Kafka.
- Spark Streaming đọc dữ liệu Kafka, ghi thô vào HDFS (real-time).
- Airflow DAG quản lý toàn bộ pipeline:
    - Trigger Spark Streaming ingest data vào HDFS.
    - Trigger Spark Batch xử lý dữ liệu từ HDFS, transform và ghi kết quả vào PostgreSQL/MongoDB.
- Dashboard ReactJS truy xuất dữ liệu từ các Database để trực quan hóa và quản lý.

---

## 5. Công nghệ sử dụng

| Thành phần      | Công nghệ                   |
|-----------------|----------------------------|
| API nguồn       | Flask, Python              |
| Streaming       | Apache Kafka, Spark        |
| Lưu trữ         | HDFS, PostgreSQL, MongoDB  |
| Xử lý ETL       | Apache Airflow             |
| Triển khai      | Docker Compose             |
| Dashboard       | ReactJS, Ant Design        |

---

<p align="center">⚡ <b>Đồ án tốt nghiệp - Customer Segment Platform</b> ⚡</p>
