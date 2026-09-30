from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 34) & F.col("session_id").isin([17, 23]))
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
    .orderBy("event_type_values")
)
