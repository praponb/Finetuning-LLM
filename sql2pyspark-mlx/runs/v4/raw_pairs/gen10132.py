from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-11-29 21:00:00"))) & F.hour("started_at").between(6, 14))
    .select(
        "*",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.length("country").alias("country_length"),
    )
)

cleaned = (
    base
    .filter(F.col("started_at") > F.to_date(F.lit("2024-03-13")))
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.upper("browser").alias("browser_upper"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.country").alias("customer_country"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") >= 209), "left_anti")
)

result = (
    not_flagged
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "browser_upper",
        "customer_country",
        F.col("max_subscription_monthly_fee"),
    )
)
