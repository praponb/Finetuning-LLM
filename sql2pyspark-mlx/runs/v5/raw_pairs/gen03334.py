from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_num", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("session_id").orderBy(F.col("last3_sum_duration_sec").asc_nulls_first(), "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/search", "/cart", "/checkout"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "session_id",
        F.count("duration_sec").over(w).alias("last3_count_duration_sec"),
        F.avg("duration_sec").over(w).alias("last3_avg_duration_sec"),
        F.sum("duration_sec").over(w).alias("last3_sum_duration_sec"),
        F.stddev_samp("duration_sec_num").over(w2).alias("session_id_stddev_samp_duration_sec_num"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .filter(F.hour(F.col("b.started_at")).between(7, 12))
    .select("src.*", F.col("b.device"), F.col("b.country"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    ranked
    .select("id")
    .intersectAll(
        df_web_events
        .filter(F.col("event_time") >= "2023-11-01")
        .select("id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("session_id_stddev_samp_duration_sec_num") >= 61, "PREMIUM").otherwise("STANDARD").alias("session_id_stddev_samp_duration_sec_num_tier"),
        F.when(F.col("last3_count_duration_sec") >= 18, "PREMIUM").otherwise("STANDARD").alias("last3_count_duration_sec_tier"),
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.round(F.avg("last3_sum_duration_sec"), 2).alias("avg_last3_sum_duration_sec"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("num_devices").desc())
)
