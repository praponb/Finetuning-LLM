from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "click", "page_view"]) & ((F.col("duration_sec") > 214) | (F.col("page") != "/cart")))
    .groupBy("session_id", "page")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy(F.col("search_duration_sec").desc())
)
