from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2024-04-08").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("days_since_start_date") >= 65, "PREMIUM").otherwise("STANDARD").alias("days_since_start_date_tier"),
        F.date_trunc("QUARTER", "end_date").alias("quarter_start"),
    )
    .agg(
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("quarter_start").desc())
)
