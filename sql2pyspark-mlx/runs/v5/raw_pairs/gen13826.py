from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
    .filter(F.col("basic_monthly_fee") >= 26)
    .select("status", "monthly_fee_range", "basic_monthly_fee")
    .orderBy(F.col("monthly_fee_range").desc())
)
