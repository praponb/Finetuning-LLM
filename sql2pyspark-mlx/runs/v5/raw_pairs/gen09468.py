from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("event_time", "id")
w4 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_max_duration_sec", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("session_id", "country").orderBy("island_id", "id")
w8 = Window.partitionBy("country").orderBy("duration_sec_count_90d", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("event_type", "session_id").orderBy("running_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.country"), F.col("p2.device"))
)

with_windows = (
    with_dims
    .filter(F.col("device") == "mobile")
    .select(
        "*",
        F.max("duration_sec").over(w_part).alias("running_max_duration_sec"),
        F.count("duration_sec").over(w_part).alias("running_count_duration_sec"),
        F.avg("duration_sec").over(w_part).alias("running_avg_duration_sec"),
        F.count("duration_sec").over(w2).alias("duration_sec_count_90d"),
    )
)

gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w4))

with_windows_web_events = (
    islands
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "running_max_duration_sec",
        "running_count_duration_sec",
        "running_avg_duration_sec",
        "duration_sec_count_90d",
        "session_start_flag",
        "island_id",
        F.sum("running_max_duration_sec").over(w5).alias("cumulative_running_max_duration_sec"),
        F.avg("running_max_duration_sec").over(w6).alias("running_max_duration_sec_avg_2_prior"),
        F.ntile(10).over(w7).alias("island_id_bucket"),
        F.avg("running_count_duration_sec").over(w8).alias("running_count_duration_sec_avg_6"),
        F.first("running_max_duration_sec", ignorenulls=True).over(w9).alias("first_known_running_max_duration_sec"),
    )
)

matched_keys = (
    with_windows_web_events
    .select("id")
    .exceptAll(
        df_web_events
        .filter(~F.col("page").isin(["/search", "/cart"]))
        .select("id")
    )
)

reconciled = (
    with_windows_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .rollup("activity_month", "island_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("first_known_running_max_duration_sec")).otherwise(0)).alias("cart_first_known_running_max_duration_sec"),
        F.max("event_time").alias("ended_at"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy(F.col("activity_month").desc())
)
