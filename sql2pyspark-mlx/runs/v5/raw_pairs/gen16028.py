from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("event_type").isin(["page_view", "purchase", "add_to_cart"]))
        .select("id", "event_type")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("event_type")
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_avg_duration_sec"),
        F.min("event_time").alias("first_seen"),
    )
    .orderBy(F.col("cart_count").desc())
)
