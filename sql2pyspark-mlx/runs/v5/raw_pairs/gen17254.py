from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("is_new_session").desc_nulls_last(), "id")
w_recent = Window.partitionBy("session_id", "browser").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("browser", "page").orderBy("rank_in_group", "id")

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

latest = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("is_new_session_2", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("is_new_session_2").over(w2))

joined = (
    sessions_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.browser"), F.col("p.device"), F.col("p.country"))
)

enriched = (
    joined
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "burst_id",
        "is_new_session_2",
        "device",
        "country",
        F.sum("burst_id").over(w_recent).alias("last4_sum_burst_id"),
        F.max("burst_id").over(w_recent).alias("last4_max_burst_id"),
        F.avg("burst_id").over(w_recent).alias("last4_avg_burst_id"),
        F.count("burst_id").over(w_recent).alias("last4_count_burst_id"),
        F.lead("session_no", 1).over(w5).alias("next_session_no"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "burst_id",
        "is_new_session_2",
        "device",
        "country",
        "last4_max_burst_id",
        "last4_avg_burst_id",
        "last4_count_burst_id",
    )
    .orderBy(F.col("last4_count_burst_id").desc())
)
