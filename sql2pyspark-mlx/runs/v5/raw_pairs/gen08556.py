from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("session_id_2", "id")
w4 = Window.partitionBy("session_id").orderBy(F.col("session_id_2").desc(), "id")
w5 = Window.partitionBy("device").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy("session_id_2_bucket", "id")
w_ordered = Window.partitionBy("browser", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("browser").orderBy(F.col("duration_sec").desc(), "id")
w9 = Window.partitionBy("browser").orderBy("cumulative_duration_sec", "id")

flagged = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

ranked = (
    sessions_web_events
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.device"), F.col("p.browser"))
)

enriched = (
    joined
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "event_type",
        "duration_sec",
        "dedup_rank",
        "browser",
        F.ntile(5).over(w4).alias("session_id_2_bucket"),
        F.sum("is_new_burst").over(w5).alias("is_new_burst_sum_7d"),
    )
)

enriched_web_events = (
    enriched
    .select(
        "id",
        "duration_sec",
        "browser",
        "session_id_2_bucket",
        F.lag("duration_sec", 1).over(w6).alias("prev_duration_sec"),
        F.sum("duration_sec").over(w_ordered).alias("cumulative_duration_sec"),
    )
)

metrics = (
    enriched_web_events
    .withColumn("duration_sec_bucket", F.ntile(4).over(w8))
    .withColumn("prev_cumulative_duration_sec", F.lag("cumulative_duration_sec", 1).over(w9))
)

daily = (
    metrics
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.sum(F.when(F.col("browser") == "Edge", F.col("duration_sec_bucket")).otherwise(0)).alias("edge_duration_sec_bucket"),
        F.size(F.collect_set("browser")).alias("num_browsers_2"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
)

result = (
    daily
    .select(
        "browser",
        "edge_duration_sec_bucket",
        "num_browsers_2",
        F.when(F.col("browser").rlike("^(Edge|Safari)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("browser_zone"),
    )
    .orderBy(F.col("browser_zone").asc_nulls_last(), "edge_duration_sec_bucket")
)
