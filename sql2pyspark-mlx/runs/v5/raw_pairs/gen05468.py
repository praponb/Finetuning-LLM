from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-04-19 01:00:00").cast("timestamp"))
    .select(
        "*",
        F.trim("device").alias("clean_device"),
        F.trim("device").alias("clean_device_2"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"), F.col("c.event_time").alias("web_event_event_time"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

enriched = (
    with_last
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("a.*", F.col("d.city"), F.col("d.email"))
)

with_last_sessions = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

result = (
    with_last_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "device"], ["segment"]],
        F.col("c.segment").alias("segment"),
        F.col("s.device").alias("device"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.web_event_duration_sec")), 2).alias("total_web_event_duration_sec"),
    )
)
