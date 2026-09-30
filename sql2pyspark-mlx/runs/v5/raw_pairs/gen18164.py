from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("country").eqNullSafe(F.lit("SG")))
    .select("*", F.length("browser").alias("browser_length"))
)

result = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < "2025-10-23"), "left_anti")
)
