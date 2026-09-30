from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-07-31"))) & F.col("page").isNotNull())
    .withColumn("clean_page", F.trim("page"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

prev_events = deduped.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

joined = (
    islands
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .filter(F.col("d.browser").isNull())
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.duration_sec",
        "s.clean_page",
        "s.latest_rank",
        "s.prev_event_time",
        F.col("d.browser"),
        F.col("d.country"),
        F.col("d.device"),
    )
)

result = (
    joined
    .groupBy("device")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        ((F.unix_timestamp(F.max("prev_event_time")) - F.unix_timestamp(F.min("prev_event_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
    )
    .orderBy(F.col("tablet_count").desc_nulls_last())
)
