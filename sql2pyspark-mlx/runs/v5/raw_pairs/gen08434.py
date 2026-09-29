from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("total_count"),
    )
)
