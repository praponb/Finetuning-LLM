from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
        F.sum(F.when(F.col("we.event_type") == "purchase", F.col("we.duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
    .orderBy("pct_checkout")
)
