from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec_percent_rank_2").desc_nulls_first(), "id")

with_windows = (
    df_web_events
    .select(
        "*",
        F.count("duration_sec").over(w).alias("event_type_count_duration_sec"),
        F.percent_rank().over(w2).alias("duration_sec_percent_rank"),
        F.percent_rank().over(w3).alias("duration_sec_percent_rank_2"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

joined = (
    ranked
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.browser"), F.col("b.device"), F.col("b.country"))
)

result = (
    joined
    .rollup(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.when(F.col("duration_sec") >= 521, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec_percent_rank", 0.5).alias("duration_sec_percent_rank_p50"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("duration_sec_percent_rank_2", [0.25, 0.5, 0.75]).alias("duration_sec_percent_rank_2_quartiles"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc())
)
