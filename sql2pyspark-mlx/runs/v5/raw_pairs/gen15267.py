from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("order_id").isin([16, 17, 36]) & (F.to_date("paid_at") < "2024-03-10"))
    .withColumn("method_filled", F.coalesce("method", F.lit("Unknown")))
)

method_summary = (
    filtered_payments
    .groupBy("method", "status")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    method_summary
    .select("method", "avg_amount", "min_amount")
    .orderBy("avg_amount")
)
