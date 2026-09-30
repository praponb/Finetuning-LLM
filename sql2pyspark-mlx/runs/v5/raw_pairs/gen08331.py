from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-08-05").cast("date"))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.month("started_at").alias("started_at_month"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)
