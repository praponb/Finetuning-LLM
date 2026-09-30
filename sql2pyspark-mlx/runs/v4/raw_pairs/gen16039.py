from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "status")
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)
