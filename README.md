# 🛒 Brazilian E-Commerce Analytics Pipeline

An end-to-end data engineering pipeline built on AWS — processing 119,140 real e-commerce orders from the Olist Brazilian E-Commerce dataset using PySpark, AWS EMR, AWS Athena, and Streamlit.

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
     Streamlit Dashboard — Live Visualizations
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Lake Storage | AWS S3 (Bronze + Silver layers) |
| Distributed Processing | PySpark on AWS EMR (Spark 4.0.2) |
| Serverless SQL | AWS Athena |
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
├── transform.py          # PySpark ETL job (runs on AWS EMR)
├── dashboard.py          # Streamlit analytics dashboard
├── olist_results.csv     # Athena query results (monthly aggregates)
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

Partitioning reduces Athena query scan cost by ~60% — only relevant partitions are scanned per query.

### 4. Streamlit Dashboard
4 interactive visualizations: Monthly Revenue Trend, Monthly Order Volume, Late Shipments Over Time, Late Shipment Rate %.

---

## 💡 Business Insights

**Growth Story:** Olist grew from 4 orders (Sep 2016) to 9,191 orders (Nov 2017) — a 2,297x increase in 14 months.

**Late Shipment Crisis:** March 2018 had a 17.4% late rate — 1 in 6 customers received orders late. February 2018 also had 1,084 late shipments. This is exactly the kind of operational signal this pipeline is designed to surface early.

**Black Friday Effect:** November 2017 was the single largest month — 9,191 orders, $1.61M revenue — nearly double October. Late shipments spiked to 1,051 (11.4%) showing logistics strain from seasonal demand.

---

## 🚀 How to Run

### Prerequisites
```bash
pip install pyspark boto3 streamlit plotly pandas
aws configure  # Enter your AWS credentials and region
```

### Run PySpark Job (requires EMR cluster)
```bash
aws s3 cp transform.py s3://your-bucket/scripts/transform.py
# Submit as EMR Step via AWS Console → Spark application
```

### Run Dashboard
```bash
streamlit run dashboard.py
```

---

## 💰 AWS Cost

Total cost: **< $1.00** (from AWS credits)

| Service | Cost |
|---|---|
| AWS S3 | ~$0.00 (free tier) |
| AWS EMR (2 runs × 15 min) | ~$0.50 |
| AWS Athena | ~$0.01 |

---

## 👩‍💻 Author

**Bhagavathi Neha Chinnam**  
B.Tech, Computer Science & Engineering (Big Data & Analytics) | SRM University AP

- GitHub: [github.com/nehachinnam956](https://github.com/nehachinnam956)
- LinkedIn: [linkedin.com/in/bhagavathi-neha-bba319291](https://www.linkedin.com/in/bhagavathi-neha-bba319291)
- Portfolio: [nehachinnam956.github.io/nehachinnam-portfolio](https://nehachinnam956.github.io/nehachinnam-portfolio/)
- LeetCode: [leetcode.com/u/NehaChinnam](https://leetcode.com/u/NehaChinnam/)
