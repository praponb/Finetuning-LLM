from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("session_id").isin([7, 38, 39]))

agg = (
    recent_web_events
    .groupBy(
        F.when(F.col("event_type").isin(["search", "add_to_cart"]), "core").otherwise("other").alias("event_type_group"),
    )
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    agg
    .orderBy("event_type_group")
    .limit(5)
)
