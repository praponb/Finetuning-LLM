from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-08-30").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

remaining_keys = (
    valid
    .select("customer_id", "plan")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date").between("2024-10-27", "2025-08-01"))
        .select("customer_id", "plan")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.substring("status", 1, 3).alias("status_prefix_2"),
    )
)

filtered = (
    cleaned
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-08-07")))
    .select("*", F.datediff(F.current_date(), "end_date").alias("days_since_end_date"))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_prefix_2",
        "s.days_since_end_date",
        F.col("d.country"),
    )
)

totals = (
    with_dims
    .groupBy(
        F.when(F.col("days_since_end_date") >= 84, "HIGH").otherwise("LOW").alias("days_since_end_date_tier"),
    )
    .agg(
        F.avg("days_since_end_date").alias("avg_days_since_end_date"),
        F.sum(F.when(F.col("country") == "US", F.col("days_since_end_date"))).alias("us_total_days_since_end_date"),
        F.max("days_since_end_date").alias("max_days_since_end_date"),
        F.sum(F.when(F.col("country") == "US", F.col("days_since_end_date")).otherwise(0)).alias("us_days_since_end_date"),
    )
)

result = (
    totals
    .select(
        "days_since_end_date_tier",
        "avg_days_since_end_date",
        "us_total_days_since_end_date",
        "max_days_since_end_date",
        "us_days_since_end_date",
    )
    .orderBy(F.col("us_days_since_end_date").desc(), "us_total_days_since_end_date")
)
