from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 2) & (F.col("duration_sec") <= 231) & (F.col("page") != "/checkout"))
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
    .orderBy("session_id")
    .limit(1)
)
