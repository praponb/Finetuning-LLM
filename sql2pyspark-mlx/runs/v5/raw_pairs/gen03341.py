from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date").between("2025-02-22", "2025-07-01") & (F.col("end_date") >= F.lit("2023-09-10").cast("date")))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.lower("plan").alias("plan_lower"),
    )
)

result = (
    base
    .cube(
        "customer_id",
        F.when(F.col("monthly_fee") >= 187, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev_2"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .orderBy(F.col("monthly_fee_tier").desc_nulls_last())
)
