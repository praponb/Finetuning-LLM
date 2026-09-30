from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-10-12").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("status", ["ACTIVE", "EXPIRED", "PAUSED"])
    .agg(F.sum("monthly_fee_whole").alias("amount"))
)
