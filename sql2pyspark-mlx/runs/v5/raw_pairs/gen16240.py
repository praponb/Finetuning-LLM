from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-06-14").cast("date")) & (F.col("event_time") >= "2023-05-01"))
    .select(
        "*",
        F.when(F.col("event_type").isin(["click", "page_view"]), "Core").otherwise("Other").alias("event_type_group"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"), F.col("p.browser"))
)

joined = (
    enriched
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.country").alias("session_country"), F.col("s.device"))
)

with_prev = joined.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "event_type_group",
        "country",
        "session_country",
        "device",
        "prev_event_time",
    )
    .orderBy(F.col("session_id").desc())
)
