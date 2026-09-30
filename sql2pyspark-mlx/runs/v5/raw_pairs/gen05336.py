from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .filter(F.col("duration_sec_bit_and") > 1)
    .orderBy(F.col("event_type").desc_nulls_first())
)
