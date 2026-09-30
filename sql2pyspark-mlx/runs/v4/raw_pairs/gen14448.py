from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("event_time").asc_nulls_first())
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("rn", "id")

with_dims = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country"), F.col("b.device"), F.col("b.browser"))
)

cleaned = (
    with_dims
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "country",
        "device",
        "browser",
        F.date_trunc("YEAR", "event_time").alias("event_time_period"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

prev_events = deduped.withColumn("prev_event_time", F.lag("event_time").over(w2))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w3))

enriched = (
    sessions_web_events
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.device").alias("session_device"))
)

deduped_web_events = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped_web_events
    .groupBy("session_id", "device")
    .pivot("event_type", ["add_to_cart", "page_view"])
    .agg(
        F.sum("session_start_flag").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
