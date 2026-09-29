from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy(F.col("total_shipping_fee").desc())
)
