from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-06-28").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

result = (
    cleaned
    .select("customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isin([4, 8, 30]))
        .select("customer_id")
    )
)
