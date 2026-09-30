from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Safari"]) & (~F.col("country").eqNullSafe(F.lit("DE"))))
    .select(
        "*",
        F.concat_ws("/", "device", "country").alias("device_country_key"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

with_window = (
    cleaned
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    with_window
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.year(F.col("c.signup_date")) >= 2023)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.country",
        "s.device_country_key",
        "s.country_clean",
        "s.subscription_id",
        "s.plan",
        F.col("c.email"),
        F.col("c.segment"),
    )
)

kept_keys = (
    joined
    .select("device", "country")
    .intersect(
        df_sessions
        .filter(F.col("started_at") < "2024-10-01")
        .select("device", "country")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupingSets(
        [["customer_segment", "device"], ["customer_segment"], []],
        F.col("d.segment").alias("customer_segment"),
        F.col("src.device").alias("device"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.subscription_id")).alias("min_subscription_id"),
    )
)
