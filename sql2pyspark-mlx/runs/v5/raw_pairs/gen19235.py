from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("customer_id").isin([4, 22, 30, 33]) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.lower("device").alias("device_lower"),
        F.concat_ws("-", "browser", "device").alias("browser_device_key"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_clean"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)
