from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "partner"]) & ((F.col("customer_id") == 34) | F.col("status").isin(["SHIPPED", "PENDING"])))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("latest_order_date")
)
