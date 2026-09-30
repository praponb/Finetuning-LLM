from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 6) & F.col("customer_id").isin([12, 15, 30]))
    .groupBy("customer_id")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
    .filter(F.col("mobile_amount") >= 20)
)
