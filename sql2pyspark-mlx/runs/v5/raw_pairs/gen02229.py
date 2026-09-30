from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("duration_sec") == 445)

event_type_group_summary = (
    filtered_web_events
    .groupBy(
        F.when(F.col("event_type").isin(["click", "purchase"]), "core").otherwise("other").alias("event_type_group"),
    )
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    event_type_group_summary
    .select("event_type_group", "avg_duration_sec")
    .orderBy("event_type_group")
    .limit(10)
)
