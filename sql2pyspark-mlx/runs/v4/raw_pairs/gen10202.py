from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.device"), F.col("s.country"))
    .agg(
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.sum(F.when(F.col("we.page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
)
