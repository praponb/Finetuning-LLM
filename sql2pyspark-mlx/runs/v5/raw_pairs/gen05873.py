from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("page")
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_avg_90d").asc_nulls_first())
w5 = Window.partitionBy("browser", "page").orderBy(F.col("duration_sec_avg_90d_dense_rank").asc_nulls_first())
w6 = Window.orderBy(F.col("duration_sec_percent_rank").desc_nulls_last())
w7 = Window.partitionBy("page").orderBy(F.col("page_stddev_samp_duration_sec_percent_rank").desc_nulls_first())
w8 = Window.partitionBy("page").orderBy(F.col("duration_sec_avg_90d_dense_rank").desc())
w9 = Window.partitionBy("page", "session_id")
w_recent = Window.partitionBy("page").orderBy(F.col("duration_sec_percent_rank_cume_dist").desc_nulls_first())

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-11").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
    )
)

kept_keys = (
    valid
    .select("id", "event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("session_id").isin([12, 25, 38]))
        .select("id", "event_type")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.device"), F.col("p.browser"), F.col("p.country"))
)

enriched = (
    joined
    .select(
        "id",
        "session_id",
        "page",
        "event_time_label",
        "browser",
        "country",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_90d"),
        F.percent_rank().over(w2).alias("duration_sec_percent_rank"),
    )
)

metrics = (
    enriched
    .withColumn("page_stddev_samp_duration_sec_percent_rank", F.stddev_samp("duration_sec_percent_rank").over(w3))
    .withColumn("duration_sec_avg_90d_dense_rank", F.dense_rank().over(w4))
)

metrics_web_events = (
    metrics
    .filter(F.col("page").isin(["/product", "/cart"]))
    .withColumn("duration_sec_avg_90d_dense_rank_percent_rank", F.percent_rank().over(w5))
    .withColumn("duration_sec_percent_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("page_stddev_samp_duration_sec_percent_rank_dense_rank", F.dense_rank().over(w7))
)

remaining_keys = (
    metrics_web_events
    .select("session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") == 153)
        .select("session_id")
    )
)

kept_rows_web_events = (
    metrics_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows_web_events
    .select(
        "session_id",
        "event_time_label",
        "browser",
        "country",
        "duration_sec_avg_90d",
        "duration_sec_percent_rank",
        "page_stddev_samp_duration_sec_percent_rank",
        "duration_sec_avg_90d_dense_rank_percent_rank",
        F.percent_rank().over(w8).alias("duration_sec_avg_90d_dense_rank_percent_rank_2"),
        F.round((F.col("page_stddev_samp_duration_sec_percent_rank_dense_rank") / F.sum("page_stddev_samp_duration_sec_percent_rank_dense_rank").over(w9)), 4).alias("page_stddev_samp_duration_sec_percent_rank_dense_rank_share"),
        F.dense_rank().over(w_recent).alias("duration_sec_percent_rank_cume_dist_dense_rank"),
    )
)
