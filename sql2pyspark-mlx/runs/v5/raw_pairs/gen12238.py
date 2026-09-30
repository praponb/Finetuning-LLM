from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .withColumn("method_filled", F.coalesce("method", F.lit("unassigned")))
)

paid_month_summary = (
    valid_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"), "status")
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
)

result = (
    paid_month_summary
    .filter(F.col("failed_count") >= 3)
    .select("paid_month", "failed_count")
    .orderBy("paid_month")
)
