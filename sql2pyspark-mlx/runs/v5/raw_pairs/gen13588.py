from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 1))
)

matched_keys = (
    filtered
    .select("plan", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").between("2023-06-21", "2024-09-01"))
        .select("plan", "status")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("a.*", F.col("d.name"))
)

cleaned = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_rounded",
        "name",
        F.upper("status").alias("status_upper"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.count("*").alias("event_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_rounded"))).alias("expired_avg_monthly_fee_rounded"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
    .filter(F.col("row_count") >= 1)
)
