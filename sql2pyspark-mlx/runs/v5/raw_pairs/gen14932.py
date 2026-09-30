from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("page_length").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy("dedup_rank", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id")
w4 = Window.orderBy(F.col("page_length").desc(), "id")
w5 = Window.partitionBy("session_country").orderBy(F.col("last3_min_duration_sec").desc())
w_recent = Window.partitionBy("page").orderBy("last3_min_duration_sec", "id")
w7 = Window.partitionBy("session_device").orderBy("last3_sum_duration_sec", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/home"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.length("page").alias("page_length"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "page_length",
        F.min("duration_sec").over(w2).alias("last3_min_duration_sec"),
        F.count("duration_sec").over(w2).alias("last3_count_duration_sec"),
        F.sum("duration_sec").over(w2).alias("last3_sum_duration_sec"),
        F.lead("page_length", 2).over(w3).alias("next_page_length"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country"), F.col("b.device"))
)

joined_web_events = (
    joined
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.country").isin(["TH", "DE", "JP"]))
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.page_length",
        "src.last3_min_duration_sec",
        "src.last3_sum_duration_sec",
        F.col("b.browser"),
        F.col("b.device").alias("session_device"),
        F.col("b.country").alias("session_country"),
    )
)

metrics = (
    joined_web_events
    .select(
        "*",
        F.ntile(4).over(w4).alias("page_length_bucket"),
        F.cume_dist().over(w5).alias("last3_min_duration_sec_cume_dist"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("last3_sum_duration_sec").over(w7).alias("last3_sum_duration_sec_avg_6"),
    )
)

result = (
    metrics
    .select(
        "id",
        "session_id",
        "page",
        "last3_min_duration_sec",
        "last3_sum_duration_sec",
        "browser",
        "session_device",
        "session_country",
        "last3_min_duration_sec_cume_dist",
    )
)
