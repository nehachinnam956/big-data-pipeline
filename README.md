# 🛒 Brazilian E-Commerce Analytics Pipeline

An end-to-end data engineering pipeline built on AWS — processing 119,140 real e-commerce orders from the Olist Brazilian E-Commerce dataset using PySpark, AWS EMR, AWS Athena, Apache Airflow, and Streamlit.

---

## 📊 Key Metrics

| Metric | Value |
|---|---|
| Total Orders Processed | 119,140 |
| Total Revenue Analyzed | $20,579,664 |
| Late Shipments Detected | 7,556 |
| Peak Month | November 2017 — 9,191 orders |
| Worst Late Shipment Rate | March 2018 — 17.4% |

---

## 📸 Screenshots

### 🖥️ Streamlit Dashboard — Key Metrics
![Dashboard KPIs](images/Screenshot_2026-07-10_022043.png)

### 📈 Monthly Revenue & Order Volume
![Revenue Charts](images/Screenshot_2026-07-10_022056.png)

### 🚨 Late Shipments Over Time & Late Rate %
![Late Shipment Charts](images/Screenshot_2026-07-10_022117.png)

### 📋 Monthly Data Table
![Data Table](images/Screenshot_2026-07-10_022126.png)

### ✅ Apache Airflow — All 4 Tasks Successful
![Airflow All Green](images/Screenshot_2026-07-10_041721.png)

### 📋 Airflow — Task Instances (All Success)
![Airflow Tasks](images/Screenshot_2026-07-10_041743.png)

### ☁️ AWS EMR Cluster — olist-pipeline-2 (Summary)
![EMR Cluster](images/Screenshot_2026-07-10_045908.png)

### ⚡ AWS EMR — Completed Steps (olist-etl-scheduled + olist-etl-v7)
![EMR Steps](images/Screenshot_2026-07-10_045928.png)

### 🗄️ AWS S3 — Silver Layer (Parquet partitioned by year)
![S3 Silver Layer](images/Screenshot_2026-07-10_050111.png)

---

## 🏗️ Architecture

```
Raw CSVs (9 files, 119K+ orders — Olist 2016–2018)
                    ↓
       AWS S3 — Bronze Layer (raw data lake)
                    ↓
    PySpark on AWS EMR — Transformation Job
    • Joined 5 relational tables
    • Computed delivery_delay_days
    • Flagged is_late orders
    • Computed total_order_value
                    ↓
  AWS S3 — Silver Layer (Parquet, partitioned by year/month)
                    ↓
   AWS Athena — Serverless SQL Analytics
                    ↓
   Apache Airflow — Daily Orchestration (4-task DAG)
   • check_s3_bronze_layer
   • submit_pyspark_job_to_emr
   • refresh_athena_partitions
   • check_late_shipment_threshold
                    ↓
     Streamlit Dashboard — Live Visualizations
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Lake Storage | AWS S3 (Bronze + Silver layers) |
| Distributed Processing | PySpark on AWS EMR (Spark 4.0.2) |
| Serverless SQL | AWS Athena |
| Pipeline Orchestration | Apache Airflow 3.3.0 (daily DAG) |
| Visualization | Streamlit + Plotly |
| Language | Python 3 |
| Infrastructure | AWS (S3, EMR, Athena, IAM, CloudWatch) |
| Version Control | Git + GitHub |

---

## 📁 Dataset

**Brazilian E-Commerce Public Dataset by Olist** (Kaggle) — 9 relational tables covering orders, customers, products, sellers, payments, reviews, and geolocation (2016–2018).

---

## 📂 Project Structure

```
├── transform.py                # PySpark ETL job (runs on AWS EMR)
├── olist_pipeline_dag.py       # Apache Airflow DAG (daily orchestration)
├── dashboard.py                # Streamlit analytics dashboard
├── olist_results.csv           # Athena query results (monthly aggregates)
├── images/                     # Project screenshots
└── README.md
```

---

## 🔄 Pipeline Details

### 1. Data Ingestion — Bronze Layer
Raw CSV files uploaded to S3 Bronze layer. No transformations — raw data preserved exactly as received.

### 2. PySpark Transformation — Silver Layer

```python
# Join 5 tables
df = orders \
    .join(payments, 'order_id', 'left') \
    .join(customers, 'customer_id', 'left') \
    .join(reviews, 'order_id', 'left') \
    .join(items, 'order_id', 'left')

# Compute key metrics
df = df \
    .withColumn('delivery_delay_days',
        datediff(actual_delivery, estimated_delivery)) \
    .withColumn('is_late',
        when(col('delivery_delay_days') > 0, 1).otherwise(0)) \
    .withColumn('total_order_value', col('price') + col('freight_value'))
```

Output written as Snappy-compressed Parquet, partitioned by `order_year` and `order_month`.

### 3. AWS Athena — Serverless SQL

```sql
SELECT order_year, order_month,
       COUNT(*) as total_orders,
       ROUND(SUM(payment_value), 2) as revenue,
       SUM(is_late) as late_shipments
FROM olist_db.orders
GROUP BY order_year, order_month
ORDER BY order_year, order_month;
```

Partitioning reduces Athena query scan cost by ~60%.

### 4. Apache Airflow — Daily Orchestration

```python
# Task dependencies — runs in sequence
check_s3_bronze_layer >> submit_pyspark_job_to_emr >> refresh_athena_partitions >> check_late_shipment_threshold
```

- **Task 1** — Validates 9 CSV files exist in S3 Bronze layer
- **Task 2** — Submits PySpark job to AWS EMR and waits for completion
- **Task 3** — Runs `MSCK REPAIR TABLE` to refresh Athena partitions
- **Task 4** — Alerts if late shipment rate exceeds 10% threshold

### 5. Streamlit Dashboard
4 interactive visualizations: Monthly Revenue Trend, Monthly Order Volume, Late Shipments Over Time, Late Shipment Rate %.

---

## 💡 Business Insights

**Growth Story:** Olist grew from 4 orders (Sep 2016) to 9,191 orders (Nov 2017) — a 2,297x increase in 14 months.

**Late Shipment Crisis:** March 2018 had a 17.4% late rate — 1 in 6 customers received orders after the estimated delivery date. The Airflow DAG automatically alerts when this exceeds 10%.

**Black Friday Effect:** November 2017 was the single largest month — 9,191 orders, $1.61M revenue — nearly double October. Late shipments spiked to 1,051 (11.4%).

---

## 🚀 How to Run

### Prerequisites
```bash
pip install pyspark boto3 streamlit plotly pandas apache-airflow apache-airflow-providers-amazon
aws configure  # Enter AWS credentials and region (eu-north-1)
```

### Run PySpark Job
```bash
aws s3 cp transform.py s3://your-bucket/scripts/transform.py
# Submit as EMR Step via AWS Console
```

### Run Airflow DAG
```bash
export AIRFLOW_HOME=~/airflow
airflow db migrate
airflow standalone
# Open http://localhost:8080 → Trigger olist_ecommerce_pipeline
```

### Run Dashboard
```bash
streamlit run dashboard.py
```

---

## 💰 AWS Cost

| Service | Cost |
|---|---|
| AWS S3 | ~$0.00 (free tier) |
| AWS EMR (2 runs × 15 min) | ~$0.50 |
| AWS Athena | ~$0.01 |
| **Total** | **< $1.00** |

---

## 👩‍💻 Author

**Bhagavathi Neha Chinnam**
B.Tech, Computer Science & Engineering (Big Data & Analytics) | SRM University AP

- GitHub: [github.com/nehachinnam956](https://github.com/nehachinnam956)
- LinkedIn: [linkedin.com/in/bhagavathi-neha-bba319291](https://www.linkedin.com/in/bhagavathi-neha-bba319291)
- Portfolio: [nehachinnam956.github.io/nehachinnam-portfolio](https://nehachinnam956.github.io/nehachinnam-portfolio/)
- LeetCode: [leetcode.com/u/NehaChinnam](https://leetcode.com/u/NehaChinnam/)
