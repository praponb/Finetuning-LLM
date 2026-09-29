from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("session_no", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy("session_no", "id")

cleaned = (
    df_web_events
    .filter(F.col("session_id").isin([7, 15, 22, 27]) & F.col("page").isNotNull())
    .select(
        "*",
        F.round((F.col("duration_sec") * 1.1), 2).alias("adjusted_duration_sec"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

scored = (
    sessions_web_events
    .filter(F.col("new_group_flag") != 0)
    .select(
        "id",
        "session_id",
        "event_time",
        "page_filled",
        "rank_in_group",
        F.avg("adjusted_duration_sec").over(w3).alias("adjusted_duration_sec_avg_6"),
        F.lag("duration_sec", 1).over(w_grp).alias("prev_duration_sec"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.device"), F.col("d.browser"))
)

pivoted = (
    joined
    .groupBy("device")
    .pivot("browser", ["Chrome", "Safari", "Firefox"])
    .agg(
        F.avg("prev_duration_sec").alias("total"),
        F.count(F.lit(1)).alias("item_count"),
    )
)

result = (
    pivoted
    .select(
        "device",
        "Chrome_total",
        "Chrome_item_count",
        "Safari_total",
        "Safari_item_count",
        "Firefox_total",
        "Firefox_item_count",
    )
)
