from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([11, 14, 23, 26]) & (F.col("status") == "FAILED"))
)

payments_stats = (
    base
    .groupBy(
        F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(F.count("*").alias("total_count"))
)

result = (
    payments_stats
    .orderBy(F.col("total_count").desc(), F.col("status_group").desc())
    .limit(5)
)
