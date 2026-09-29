from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.min("discount").alias("min_discount"),
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .filter(F.col("min_discount") > 0.15)
    .select("status", "min_discount", "mobile_count")
    .orderBy("min_discount")
)
