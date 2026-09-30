from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([21, 22, 26, 29]))
    .groupBy("status")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date"),
    )
)
