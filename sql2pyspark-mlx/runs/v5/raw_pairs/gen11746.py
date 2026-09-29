from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-24 16:00:00").cast("timestamp")) & (F.col("device") == "mobile"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
        F.upper("browser").alias("browser_upper"),
        F.coalesce("country", F.lit("NONE")).alias("country_filled_2"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
