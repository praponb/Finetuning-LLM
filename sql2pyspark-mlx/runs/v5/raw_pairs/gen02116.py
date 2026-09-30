from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w3 = Window.partitionBy("page").orderBy("last4_count_duration_sec")
w_recent = Window.partitionBy("session_id", "country").orderBy(F.col("duration_sec").desc(), "id")
w_grp = Window.partitionBy("country").orderBy("last4_avg_duration_sec", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("country", "session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy(F.col("last4_avg_duration_sec").desc_nulls_last())

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.concat("event_type", F.lit(" - "), "page").alias("event_type_page"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.min("duration_sec").over(w).alias("last4_min_duration_sec"),
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.count("duration_sec").over(w).alias("last4_count_duration_sec"),
        F.max("duration_sec").over(w).alias("last4_max_duration_sec"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.event_time",
        "src.duration_sec",
        "src.last4_avg_duration_sec",
        F.col("b.country"),
        F.col("b.browser"),
    )
)

scored = (
    enriched
    .filter(~F.col("country").isin(["TH", "SG", "JP"]))
    .select(
        "*",
        F.ntile(3).over(w_recent).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w_grp).alias("duration_sec_avg_2"),
        F.avg("last4_avg_duration_sec").over(w6).alias("last4_avg_duration_sec_avg_2"),
        F.rank().over(w7).alias("last4_avg_duration_sec_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "duration_sec",
        "last4_avg_duration_sec",
        "country",
        "browser",
        "duration_sec_bucket",
        "duration_sec_avg_2",
        "last4_avg_duration_sec_avg_2",
    )
)
