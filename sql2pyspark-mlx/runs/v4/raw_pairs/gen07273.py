from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-07-18 04:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

result = (
    cleaned
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)
