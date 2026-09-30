from pyspark.sql import functions as F

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (~F.col("ch.event_type").isin(["search", "add_to_cart"])), "left_anti")
)

result = (
    not_flagged
    .groupBy("browser", "country")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.countDistinct("device").alias("distinct_devices"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("distinct_devices") < 1000)
)
