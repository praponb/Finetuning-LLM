from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 730)
    .select("id", "customer_id", "store_id", "order_date")
    .orderBy(F.col("customer_id").desc())
)
