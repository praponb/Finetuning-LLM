from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

matched_keys = (
    df_sessions
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Chrome"]))
        .select("device")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-08-19 03:00:00"))) & F.col("customer_id").isNotNull() & F.col("country").isin(["US", "UK", "JP"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"))
)

kept = (
    joined
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)

joined_sessions = (
    kept
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.credit_limit"),
        F.col("d.country").alias("customer_country"),
        F.col("d.name"),
    )
)

valid_sessions = (
    joined_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

result = (
    valid_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "is_mobile",
        "city",
        "name",
        "city_filled",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("started_at").asc_nulls_first())
)
