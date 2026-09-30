from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.19)
    .groupBy("status")
    .agg(
        F.count("discount").alias("discount_count"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
)
