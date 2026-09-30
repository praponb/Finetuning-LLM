from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([16, 29, 35, 38]) & (F.col("duration_sec") >= 346) & (F.col("event_type") != "add_to_cart"))
    .groupBy(
        "event_type",
        F.when(F.col("page").isin(["/checkout", "/product"]), "tier_1").otherwise("other").alias("page_group"),
        "session_id",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy("page_group", "search_duration_sec")
)
