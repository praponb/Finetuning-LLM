from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("duration_sec_range") >= 59)
)
