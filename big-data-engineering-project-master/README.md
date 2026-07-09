# Big Data ETL Pipeline — Brazilian E-Commerce Analytics

End-to-end data engineering pipeline on AWS for large-scale e-commerce data processing, analytics, and business insights using PySpark, AWS EMR, S3, Athena, and Apache Airflow.

---

## Architecture

```
Raw Data (Kaggle Olist Dataset)
        ↓
AWS S3 — Bronze Layer (raw CSV files)
        ↓
PySpark on AWS EMR — Silver Layer (clean, transform, join)
        ↓
AWS Athena — Gold Layer (serverless SQL analytics)
        ↓
Apache Airflow — Orchestration & Scheduling
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Processing | Python, PySpark |
| Distributed Computing | AWS EMR |
| Data Lake Storage | AWS S3 |
| Serverless SQL Analytics | AWS Athena |
| Pipeline Orchestration | Apache Airflow |
| Data Transformation | Spark SQL, Pandas |
| Version Control | Git, GitHub |

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** (Kaggle)
- 100,000+ orders from 2016 to 2018
- 9 relational tables: orders, customers, products, sellers, payments, reviews, geolocation, order items, product category translations
- Real-world B2C e-commerce data from Brazil's largest department store marketplace

---

## What This Pipeline Does

### 1. Data Ingestion (`s3_upload.py`)
- Downloads raw CSV files from local storage
- Uploads to AWS S3 Bronze layer with structured folder partitioning
- Handles multiple relational tables with error logging

### 2. PySpark Transformation on AWS EMR (`spark_missed_deadline_job.py`)
- Cleans null values, standardizes date formats, deduplicates records
- Joins 6 relational tables: orders + customers + products + payments + sellers + reviews
- Computes derived metrics:
  - Delivery delay (estimated vs actual delivery date)
  - Order total value
  - Customer lifetime value (LTV)
  - Seller performance score
- Partitions output by order date and product category for cost-optimized Athena queries

### 3. Serverless SQL Analytics (`AWS Athena`)
- Queries processed S3 data directly — no database loading required
- Optimized partitioning reduces query scan cost by ~60%
- Key analytical queries:
  - Late shipment detection
  - Monthly revenue trends
  - Seller ranking by delivery performance
  - Top product categories by revenue

### 4. Pipeline Orchestration (`late_shipments_to_carrier_dag.py`)
- Apache Airflow DAG automates the full pipeline on a daily schedule
- Monitors: S3 upload → EMR Spark job → Athena table refresh
- Sends alerts when late shipment threshold is exceeded
- Handles retries and failure notifications automatically

---

## Key Business Insights

- **Late Shipment Detection** — Identifies orders at risk of missing carrier deadlines so logistics teams can act proactively
- **Revenue Trend Analysis** — Monthly and category-level revenue patterns to support go-to-market decisions
- **Seller Performance Scoring** — Ranks sellers by delivery speed, on-time rate, and review scores
- **Customer Segmentation** — RFM analysis (Recency, Frequency, Monetary) for targeted marketing
- **Anomaly Detection** — Flags unusual spikes in order cancellations or delivery failures

---

## Exploratory Data Analysis

See `Data/Brazilian ecommerce EDA.ipynb` for full EDA including:
- Order volume trends over time
- Revenue distribution by product category
- Delivery time analysis by region
- Customer review sentiment distribution
- Payment method breakdown

---

## Project Structure

```
├── Data/
│   ├── Brazilian ecommerce EDA.ipynb   # Full exploratory data analysis
│   └── brazilian-ecommerce.zip         # Raw Olist dataset
├── airflow/
│   ├── dags/
│   │   └── late_shipments_to_carrier_dag.py   # Airflow DAG definition
│   └── scripts/
│       ├── s3_upload.py                # Uploads raw data to S3
│       ├── s3_download.py              # Downloads processed data from S3
│       └── spark_missed_deadline_job.py # PySpark transformation job
└── README.md
```

---

## Setup & Installation

### Prerequisites
```bash
pip install apache-airflow pyspark boto3 pandas
```

### AWS Configuration
```bash
aws configure
# Enter: AWS Access Key, Secret Key, Region (ap-south-1 or us-east-1)
```

### Run the Pipeline

**Step 1 — Upload raw data to S3**
```bash
python airflow/scripts/s3_upload.py
```

**Step 2 — Start Airflow and trigger DAG**
```bash
airflow standalone
airflow dags trigger late_shipments_to_carrier_dag
```

**Step 3 — Query processed data with Athena**
```sql
-- Late shipment detection
SELECT 
    seller_id,
    COUNT(*) AS late_shipments,
    AVG(DATEDIFF(actual_delivery, estimated_delivery)) AS avg_delay_days
FROM processed_orders
WHERE actual_delivery > estimated_delivery
GROUP BY seller_id
ORDER BY late_shipments DESC
LIMIT 20;

-- Monthly revenue trend
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS month,
    SUM(payment_value) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders
FROM processed_orders
GROUP BY 1
ORDER BY 1;
```

---

## Skills Demonstrated

`Python` `PySpark` `Apache Spark` `AWS S3` `AWS EMR` `AWS Athena`  
`Apache Airflow` `SQL` `ETL Pipeline` `Data Lake Architecture`  
`Distributed Processing` `Big Data` `EDA` `Data Quality` `Data Engineering`

---

## Author

**Bhagavathi Neha Chinnam**
B.Tech, Computer Science & Engineering (Big Data & Analytics) | SRM University AP

- GitHub: [github.com/nehachinnam956](https://github.com/nehachinnam956)
- LinkedIn: [linkedin.com/in/bhagavathi-neha-bba319291](https://www.linkedin.com/in/bhagavathi-neha-bba319291)
- Portfolio: [nehachinnam956.github.io/nehachinnam-portfolio](https://nehachinnam956.github.io/nehachinnam-portfolio/)
- LeetCode: [leetcode.com/u/NehaChinnam](https://leetcode.com/u/NehaChinnam/)
