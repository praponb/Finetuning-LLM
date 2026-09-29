from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup(
        F.when(F.col("duration_sec") >= 398, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "event_type",
    )
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "page_view", 1)), F.lit(0)), 2).alias("duration_sec_per_page_view"),
    )
    .filter(F.col("num_event_types") >= 1)
)
