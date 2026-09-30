from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy(F.col("new_group_flag").desc(), "id")
w4 = Window.partitionBy("browser").orderBy(F.col("session_id").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

with_windows = (
    sessions_sessions
    .withColumn("new_group_flag_bucket", F.ntile(5).over(w3))
    .withColumn("session_id_bucket", F.ntile(5).over(w4))
)

with_gaps_2 = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps_2.withColumn("island_id", F.sum("is_new_burst").over(w2))

latest_match = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

with_gaps_3 = (
    latest_match
    .withColumn("new_group_flag_2", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands_sessions = with_gaps_3.withColumn("island_id_2", F.sum("new_group_flag_2").over(w2))

cleaned = (
    islands_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2024-01-23 06:00:00"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "new_group_flag",
        "session_id",
        "new_group_flag_bucket",
        "session_id_bucket",
        "new_group_flag_2",
        "island_id_2",
        (F.col("new_group_flag_2") * F.col("island_id")).alias("new_group_flag_2_times_island_id"),
    )
)

by_type = (
    cleaned
    .groupBy("device")
    .pivot("browser", ["Chrome", "Firefox"])
    .agg(F.sum("session_id").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    by_type
    .select(
        "device",
        "Firefox_value_sum",
        F.when(F.col("device").rlike("^(tablet|mobile)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("device_zone"),
    )
    .orderBy(F.col("Firefox_value_sum").desc_nulls_first(), F.col("device").desc())
)
