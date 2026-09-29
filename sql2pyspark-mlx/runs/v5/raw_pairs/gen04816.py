from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("device").orderBy("last4_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device")
w6 = Window.partitionBy("page", "device").orderBy("country_avg_duration_sec", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("page").orderBy("event_time", "id")
w8 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_web_events
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    ranked
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.browser").isin(["Firefox", "Edge", "Safari"]))
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.event_time",
        "e.duration_sec",
        F.col("d.country"),
        F.col("d.device"),
    )
)

metrics = (
    enriched
    .withColumn("last4_count_duration_sec", F.count("duration_sec").over(w2))
    .withColumn("last4_avg_duration_sec", F.avg("duration_sec").over(w2))
    .withColumn("last4_max_duration_sec", F.max("duration_sec").over(w2))
    .withColumn("country_avg_duration_sec", F.avg("duration_sec").over(w3))
)

with_windows = (
    metrics
    .select(
        "*",
        F.sum("last4_count_duration_sec").over(w4).alias("cumulative_last4_count_duration_sec"),
        F.avg("last4_avg_duration_sec").over(w5).alias("device_avg_last4_avg_duration_sec"),
        F.avg("country_avg_duration_sec").over(w6).alias("country_avg_duration_sec_avg_6_prior"),
    )
)

flagged = (
    with_windows
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w7) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "last4_avg_duration_sec",
        "country_avg_duration_sec",
        "cumulative_last4_count_duration_sec",
        "country_avg_duration_sec_avg_6_prior",
        F.sum("session_start_flag").over(w8).alias("session_no"),
    )
)

result = (
    islands
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .groupingSets(
        [["browser", "page"], ["browser"], []],
        F.col("p.browser").alias("browser"),
        F.col("s.page").alias("page"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.last4_avg_duration_sec")).alias("total_last4_avg_duration_sec"),
    )
)
