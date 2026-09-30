from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("device").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("event_type")

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        F.col("b.device"),
        F.col("b.country"),
        F.col("b.browser"),
    )
)

remaining_keys = (
    enriched
    .select("session_id", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id").isin([14, 36]))
        .select("session_id", "event_type")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

with_windows = verified.select("*", F.percent_rank().over(w).alias("duration_sec_percent_rank"))

enriched_web_events = (
    with_windows
    .withColumn("duration_sec_max_30d", F.max("duration_sec").over(w2))
    .withColumn("duration_sec_sum_7d", F.sum("duration_sec").over(w3))
    .withColumn("event_type_min_duration_sec_percent_rank", F.min("duration_sec_percent_rank").over(w4))
)

result = (
    enriched_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["session_browser", "session_country"], ["session_browser"], []],
        F.col("p.browser").alias("session_browser"),
        F.col("p.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.duration_sec_max_30d")).alias("avg_duration_sec_max_30d"),
        F.max(F.col("src.duration_sec_percent_rank")).alias("max_duration_sec_percent_rank"),
    )
    .orderBy(F.col("session_country").asc_nulls_last())
)
