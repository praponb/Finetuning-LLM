from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("duration_sec") < 430) & F.col("page").isNotNull()) | (F.col("page") == "/cart"))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(F.col("search_count").desc())
)
