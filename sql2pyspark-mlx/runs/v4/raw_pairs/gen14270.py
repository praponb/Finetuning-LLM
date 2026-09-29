from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 251)
    .groupBy(
        F.when(F.col("duration_sec") >= 437, "high").when(F.col("duration_sec") >= 172, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy("duration_sec_count")
)
