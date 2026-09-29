from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("prev_started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy(F.col("prev_started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("prev_started_at", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("browser").orderBy("started_at", "id")
w7 = Window.orderBy("session_start_flag", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("country") != "JP")
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat_ws("-", "device", "country").alias("device_country_key"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

with_prev = filtered.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

metrics = (
    islands
    .filter(F.col("prev_started_at") < "2023-03-09")
    .select(
        "id",
        "browser",
        "device_country_key",
        "session_start_flag",
        F.sum("session_id").over(w3).alias("session_id_sum_90d"),
        F.count("session_id").over(w4).alias("session_id_count_90d"),
        F.avg("session_id").over(w_running).alias("session_id_avg_6_prior"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.avg("session_id").over(w7).alias("session_id_avg_5"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.session_start_flag",
        "s.session_id_sum_90d",
        "s.session_id_count_90d",
        "s.session_id_avg_6_prior",
        "s.event_seq",
        "s.session_id_avg_5",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

with_latest_sessions = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.session_id_avg_6_prior",
        "s.web_event_duration_sec",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

result = (
    with_latest_sessions
    .groupBy(
        F.when(F.col("web_event_duration_sec") >= 476, "HIGH").otherwise("LOW").alias("web_event_duration_sec_tier"),
        "browser",
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Firefox", F.col("session_id_avg_6_prior")).otherwise(0)).alias("firefox_session_id_avg_6_prior"),
        F.percentile_approx("web_event_duration_sec", [0.25, 0.5, 0.75]).alias("web_event_duration_sec_quartiles"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
    )
    .orderBy(F.col("browser").desc())
)
