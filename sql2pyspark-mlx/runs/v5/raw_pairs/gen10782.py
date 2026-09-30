from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("event_time").desc_nulls_last(), "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

latest = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "session_id", "event_type", "page", "event_time", "duration_sec")
)

enriched = (
    latest
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isin([15, 40]))
    .select("a.*", F.col("d.device"), F.col("d.country"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "device",
        "country",
        F.sum("session_start_flag").over(w4).alias("session_no"),
    )
)

joined = (
    sessionized
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.country").alias("session_country"), F.col("d.browser"))
)

result = (
    joined
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "device",
        "country",
        "session_no",
        "session_country",
        F.col("min_web_event_duration_sec"),
    )
)
