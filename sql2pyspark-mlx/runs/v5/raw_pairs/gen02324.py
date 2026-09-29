from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
        F.trim("device").alias("clean_device"),
    )
)

result = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)
