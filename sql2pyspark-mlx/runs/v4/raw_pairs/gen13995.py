from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .select("id", "customer_id", "device", "browser")
    .orderBy(F.col("customer_id").asc_nulls_first())
)
