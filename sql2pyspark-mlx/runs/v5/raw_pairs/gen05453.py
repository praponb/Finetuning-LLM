from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("web_event_event_type").orderBy(F.col("duration_sec").desc(), "id")

scored = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        F.last("country", ignorenulls=True).over(w_running).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w_grp).alias("first_known_browser"),
    )
)

base = (
    scored
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "device",
        "browser",
        "first_known_browser",
        F.substring("device", 1, 2).alias("device_prefix"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec").between(220, 273)), "left_anti")
)

cleaned = (
    eligible
    .filter(~F.col("browser").eqNullSafe(F.lit("Edge")))
    .select("id", "first_known_browser", F.upper("browser").alias("browser_upper"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.first_known_browser",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.duration_sec"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.to_date(F.col("ch.event_time")) >= "2024-01-04"), "left_anti")
)

latest = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("duration_sec") >= 444, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.size(F.collect_set("web_event_event_type")).alias("num_web_event_event_types"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("avg_duration_sec") > 0)
)
