from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile"]))
    .withColumn("started_at_hour", F.hour("started_at"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("OTHER")))
)

result = (
    base
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)
