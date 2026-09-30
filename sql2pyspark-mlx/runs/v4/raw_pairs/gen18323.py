from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec_filled").desc_nulls_last(), "id")
w_recent = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec_filled").desc_nulls_first())
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("page")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .withColumn("duration_sec_filled_rank", F.rank().over(w_recent))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w4))
    .withColumn("page_avg_duration_sec", F.avg("duration_sec").over(w_part))
)

enriched = (
    scored
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.browser"), F.col("b.country"))
)

enriched_web_events = (
    enriched
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.country").alias("session_country"),
        F.col("p.browser").alias("session_browser"),
        F.col("p.device"),
    )
)

result = (
    enriched_web_events
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_filled",
        "rn",
        "duration_sec_filled_rank",
        "cumulative_duration_sec",
        "page_avg_duration_sec",
    )
    .orderBy(F.col("rn").asc_nulls_first())
)
