from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
)
