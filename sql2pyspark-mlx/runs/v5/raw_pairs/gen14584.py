from pyspark.sql import functions as F

result = (
    df_web_events
    .groupingSets([["event_type", "page"], ["event_type"], []], "event_type", "page")
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.countDistinct("page").alias("distinct_pages"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
)
