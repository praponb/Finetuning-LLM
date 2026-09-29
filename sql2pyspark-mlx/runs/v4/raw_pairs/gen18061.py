from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        F.when(F.col("browser") == "Safari", 1).otherwise(0).alias("is_safari"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)
