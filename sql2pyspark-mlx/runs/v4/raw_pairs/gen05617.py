from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("start_date") < "2025-09-01") & (F.col("monthly_fee").isNull() | (F.col("end_date") > "2023-11-01")))
)

summary = (
    base
    .groupBy(
        F.when(F.col("monthly_fee") >= 243, "high").when(F.col("monthly_fee") >= 77, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = (
    summary
    .filter(F.col("total_monthly_fee") < 700)
    .select("monthly_fee_bucket", "status", "total_monthly_fee")
)
