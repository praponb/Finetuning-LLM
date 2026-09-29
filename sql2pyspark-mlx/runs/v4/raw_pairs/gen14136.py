from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
    )
    .orderBy("event_type_values", "customer_id")
    .limit(5)
)
