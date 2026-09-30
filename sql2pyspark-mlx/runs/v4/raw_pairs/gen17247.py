from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc())
w_hist = Window.partitionBy("event_type")
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec_filled").desc())
w5 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w6 = Window.partitionBy("page").orderBy(F.col("event_type_stddev_samp_event_type_length").desc())
w7 = Window.partitionBy("page")

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-10-06 16:00:00").cast("timestamp")) & (F.col("session_id") == 40))
    .withColumn("duration_sec_whole", F.ceil("duration_sec"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("NONE")))
)

remaining_keys = (
    cleaned
    .select("page", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("page") == "/product")
        .select("page", "event_type")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("duration_sec_whole") < 60)
    .select(
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "resolved_page",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.length("event_type").alias("event_type_length"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page_2"),
    )
)

scored = (
    base
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w))
    .withColumn("event_type_sum_event_type_length", F.sum("event_type_length").over(w_hist))
    .withColumn("event_type_stddev_samp_event_type_length", F.stddev_samp("event_type_length").over(w_hist))
    .withColumn("duration_sec_percent_rank_2", F.percent_rank().over(w3))
    .withColumn("duration_sec_filled_cume_dist", F.cume_dist().over(w4))
)

metrics = (
    scored
    .withColumn("duration_sec_rank", F.rank().over(w5))
    .withColumn("event_type_stddev_samp_event_type_length_dense_rank", F.dense_rank().over(w6))
    .withColumn("page_sum_duration_sec_percent_rank_2", F.sum("duration_sec_percent_rank_2").over(w7))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.browser"), F.col("d.country"), F.col("d.device"))
)

base_web_events = (
    with_dims
    .filter(F.col("duration_sec") <= 380)
    .select(
        "*",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
)

result = (
    base_web_events
    .select(
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "resolved_page",
        "duration_sec_filled",
        "event_type_length",
        "duration_sec_percent_rank",
        "event_type_stddev_samp_event_type_length",
    )
    .orderBy(
        F.col("event_type_stddev_samp_event_type_length").asc_nulls_last(),
        "event_type_length",
    )
)
