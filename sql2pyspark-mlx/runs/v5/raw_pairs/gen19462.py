from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isin([10, 14, 35]))
        .select("customer_id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("browser").isNotNull())
    .withColumn("device_upper", F.upper("device"))
    .withColumn("started_at_period", F.date_trunc("year", "started_at"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (~F.col("x.page").isin(["/checkout", "/search", "/home"])), "left_anti")
)

result = (
    kept
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "started_at_period",
        F.col("avg_subscription_monthly_fee"),
    )
)
