from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum("discount").alias("total_discount"),
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.min("order_date").alias("min_order_date"),
    )
)
