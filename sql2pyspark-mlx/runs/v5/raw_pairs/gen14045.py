from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("completed_count") >= 53)
    .orderBy(F.col("completed_count").desc())
)
