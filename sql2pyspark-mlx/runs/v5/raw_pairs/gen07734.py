from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("device").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("web_event_event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("duration_sec_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("browser").isNotNull() & F.col("customer_id").isin([7, 10]))
    .select(
        "id",
        "customer_id",
        "device",
        F.substring("country", 1, 4).alias("country_prefix"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

scored = (
    with_latest
    .select(
        "id",
        "device",
        "country_prefix",
        "duration_sec",
        F.avg("duration_sec").over(w_recent).alias("duration_sec_avg_3"),
        F.row_number().over(w_part).alias("event_seq"),
        F.last("web_event_event_type", ignorenulls=True).over(w4).alias("last_known_web_event_event_type"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < "2024-08-08"), "left_anti")
)

scored_sessions = (
    kept
    .filter(F.col("device") != "desktop")
    .withColumn("running_avg_duration_sec_avg_3", F.avg("duration_sec_avg_3").over(w5))
    .withColumn("running_max_duration_sec_avg_3", F.max("duration_sec_avg_3").over(w5))
    .withColumn("running_count_duration_sec_avg_3", F.count("duration_sec_avg_3").over(w5))
)

totals = (
    scored_sessions
    .groupBy("device")
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.sum(F.when(F.col("device") == "mobile", F.col("running_count_duration_sec_avg_3"))).alias("mobile_total_running_count_duration_sec_avg_3"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("mobile_events") >= 1)
)

result = (
    totals
    .select(
        "device",
        "event_count",
        "mobile_events",
        "mobile_total_running_count_duration_sec_avg_3",
        F.when(F.col("device").rlike("^(tablet|mobile)$"), "OTHER_ZONE").when(F.col("device").rlike("^(desktop)$"), "PRIORITY").otherwise("EXTENDED").alias("device_zone"),
    )
)
