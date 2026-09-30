from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .orderBy("search_duration_sec")
)
