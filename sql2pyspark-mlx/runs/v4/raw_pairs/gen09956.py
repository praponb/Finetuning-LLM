from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "PENDING")
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.count("discount").alias("discount_count"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("avg_discount") >= 0.34)
    .drop("avg_discount")
    .orderBy(F.col("discount_count").desc())
)
