from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1)
    .drop("row_count")
)
