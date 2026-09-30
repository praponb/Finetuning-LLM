from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2025-03-22", "2025-06-01"))
    .groupBy("plan", F.month("start_date").alias("start_month"))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") > 1340)
    .drop("total_monthly_fee")
)
