from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") != "REFUNDED") & F.hour("paid_at").between(11, 18)) | (F.col("paid_at") >= "2025-08-29"))
    .groupBy("method", F.to_date("paid_at").alias("paid_day"), "status")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
    )
    .orderBy(F.col("avg_amount").desc())
)
