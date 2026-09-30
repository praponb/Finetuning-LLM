from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-06-30 05:00:00").cast("timestamp"))
    .withColumn("country_length", F.length("country"))
)

matched = (
    filtered
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

cleaned = (
    matched
    .filter(F.col("customer_id").isin([7, 8, 29, 40]))
    .select(
        "*",
        F.coalesce("subscription_id", F.lit(0)).alias("subscription_id_filled"),
        F.coalesce("plan", F.lit("OTHER")).alias("plan_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        "customer_id",
        "country",
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.sum("subscription_id_filled").alias("total_subscription_id_filled"),
        F.sum(F.when(F.col("plan") == "premium", F.col("country_length"))).alias("premium_total_country_length"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("total_subscription_id_filled") < 0)
    .orderBy("country")
)
