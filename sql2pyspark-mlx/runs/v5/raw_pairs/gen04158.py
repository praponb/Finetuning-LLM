from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-11-09").cast("date"))
    .select("*", F.ceil("monthly_fee").alias("monthly_fee_whole"))
)

result = (
    filtered
    .rollup(
        F.when(F.col("monthly_fee_whole") >= 75, "LARGE").otherwise("SMALL").alias("monthly_fee_whole_tier"),
        "plan",
        F.when(F.col("monthly_fee") >= 254, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_per_paused"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee_whole"))).alias("cancelled_total_monthly_fee_whole"),
    )
    .filter(F.col("cancelled_total_monthly_fee_whole") < 1000000)
    .orderBy("plan")
)
