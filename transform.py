from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_date, when, month, year

print(">>> STEP 1: Starting Spark")

spark = SparkSession.builder \
    .appName("OlistETL") \
    .getOrCreate()

BUCKET = "list-ecommerce-neha"
BRONZE = f"s3://{BUCKET}/bronze"
SILVER = f"s3://{BUCKET}/silver"

print(">>> STEP 2: Spark started successfully")

try:
    print(">>> STEP 3: Reading orders CSV")
    orders = spark.read.csv(
        f"{BRONZE}/olist_orders_dataset.csv",
        header=True, inferSchema=True
    )
    print(f">>> STEP 3 DONE: {orders.count()} orders loaded")

    print(">>> STEP 4: Reading payments CSV")
    payments = spark.read.csv(
        f"{BRONZE}/olist_order_payments_dataset.csv",
        header=True, inferSchema=True
    )
    print(f">>> STEP 4 DONE: {payments.count()} payments loaded")

    print(">>> STEP 5: Reading customers CSV")
    customers = spark.read.csv(
        f"{BRONZE}/olist_customers_dataset.csv",
        header=True, inferSchema=True
    )
    print(f">>> STEP 5 DONE: {customers.count()} customers loaded")

    print(">>> STEP 6: Reading reviews CSV")
    reviews = spark.read.csv(
        f"{BRONZE}/olist_order_reviews_dataset.csv",
        header=True, inferSchema=True
    )
    print(f">>> STEP 6 DONE: {reviews.count()} reviews loaded")

    print(">>> STEP 7: Reading items CSV")
    items = spark.read.csv(
        f"{BRONZE}/olist_order_items_dataset.csv",
        header=True, inferSchema=True
    )
    print(f">>> STEP 7 DONE: {items.count()} items loaded")

    print(">>> STEP 8: Joining tables")
    df = orders \
        .join(payments.select('order_id', 'payment_value', 'payment_type'),
              'order_id', 'left') \
        .join(customers.select('customer_id', 'customer_city', 'customer_state'),
              'customer_id', 'left') \
        .join(reviews.select('order_id', 'review_score'),
              'order_id', 'left') \
        .join(items.select('order_id', 'seller_id', 'price', 'freight_value'),
              'order_id', 'left')
    print(f">>> STEP 8 DONE: {df.count()} rows after join")

    print(">>> STEP 9: Adding computed columns")
    df = df \
        .withColumn('order_purchase_date',
                    to_date(col('order_purchase_timestamp'))) \
        .withColumn('order_year',
                    year(col('order_purchase_date'))) \
        .withColumn('order_month',
                    month(col('order_purchase_date'))) \
        .withColumn('delivery_delay_days',
                    datediff(
                        to_date(col('order_delivered_customer_date')),
                        to_date(col('order_estimated_delivery_date'))
                    )) \
        .withColumn('is_late',
                    when(col('delivery_delay_days') > 0, 1).otherwise(0)) \
        .withColumn('total_order_value',
                    col('price') + col('freight_value'))
    print(">>> STEP 9 DONE")

    print(">>> STEP 10: Dropping nulls")
    df = df.dropna(subset=['order_id', 'order_purchase_date', 'payment_value'])
    print(f">>> STEP 10 DONE: {df.count()} clean rows")

    print(">>> STEP 11: Writing Silver layer to S3")
    df.write \
      .mode('overwrite') \
      .partitionBy('order_year', 'order_month') \
      .parquet(f"{SILVER}/orders/")
    print(">>> STEP 11 DONE: Silver layer written!")

except Exception as e:
    print(f">>> FAILED AT STEP: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

spark.stop()
print(">>> PIPELINE COMPLETE!")