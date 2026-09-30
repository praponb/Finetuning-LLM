from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy("duration_sec_range")
)
