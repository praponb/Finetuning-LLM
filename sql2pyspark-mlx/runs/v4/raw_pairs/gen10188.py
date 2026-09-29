from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-05-28").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    cleaned
    .select("customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
    )
)
