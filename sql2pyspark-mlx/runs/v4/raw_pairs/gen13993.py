from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec_filled").desc_nulls_last(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec_filled").asc_nulls_last())
w3 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("duration_sec", "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_filled").desc(), "id")
w6 = Window.partitionBy("session_id")
w7 = Window.orderBy("event_seq", "id")
w8 = Window.partitionBy("device").orderBy("session_id_avg_duration_sec_filled", "id")
w9 = Window.partitionBy("device").orderBy("duration_sec_filled", "id")
w10 = Window.partitionBy("session_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-11-15 22:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.browser"), F.col("s.country"), F.col("s.device"))
)

base = (
    enriched
    .filter(F.col("session_id").isin([4, 5, 13, 23]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "event_time_label",
        "duration_sec_filled",
        F.upper("country").alias("country_upper"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.device"))
)

enriched_web_events = (
    joined
    .select(
        "*",
        F.cume_dist().over(w2).alias("duration_sec_filled_cume_dist"),
        F.sum("duration_sec_filled").over(w3).alias("running_duration_sec_filled"),
        F.lead("duration_sec", 2).over(w4).alias("next_duration_sec"),
        F.row_number().over(w5).alias("event_seq"),
        F.avg("duration_sec_filled").over(w6).alias("session_id_avg_duration_sec_filled"),
    )
)

with_windows = (
    enriched_web_events
    .select(
        "*",
        F.ntile(10).over(w7).alias("event_seq_bucket"),
        F.lag("duration_sec_filled_cume_dist", 2).over(w8).alias("prev_duration_sec_filled_cume_dist"),
        F.lead("duration_sec_filled", 2).over(w9).alias("next_duration_sec_filled"),
        F.sum("duration_sec_filled").over(w10).alias("running_duration_sec_filled_2"),
    )
)

stats = (
    with_windows
    .groupBy("device")
    .agg(
        F.round(F.sum("prev_duration_sec_filled_cume_dist") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("prev_duration_sec_filled_cume_dist_per_home"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    stats
    .select("device", "prev_duration_sec_filled_cume_dist_per_home", "event_type_list")
    .orderBy(F.col("device").asc_nulls_last())
)
