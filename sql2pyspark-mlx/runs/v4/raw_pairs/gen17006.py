from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.to_date("event_time") >= "2024-04-01") & F.col("duration_sec").between(175, 177)) | F.col("event_type").isin(["click", "add_to_cart", "search"]))
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)
