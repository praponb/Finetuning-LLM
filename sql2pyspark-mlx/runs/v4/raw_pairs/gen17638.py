from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull() & F.col("device").isin(["tablet", "desktop"]))
    .select("*", F.coalesce("country", F.lit("N/A")).alias("country_filled"))
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)
