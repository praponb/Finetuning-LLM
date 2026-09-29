from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 24)
    .groupBy("event_type", "page")
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .filter(F.col("product_count") < 7)
    .select("event_type", "page", "product_count", "page_list")
    .orderBy("page", "event_type")
)
