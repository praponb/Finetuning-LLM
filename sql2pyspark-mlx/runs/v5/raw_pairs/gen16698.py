from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at_hour", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("device").orderBy(F.col("started_at_hour").desc(), "id")

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.hour("started_at").alias("started_at_hour"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "started_at_hour",
        F.avg("started_at_hour").over(w).alias("last7_avg_started_at_hour"),
        F.max("started_at_hour").over(w).alias("last7_max_started_at_hour"),
        F.min("started_at_hour").over(w).alias("last7_min_started_at_hour"),
        F.sum("started_at_hour").over(w).alias("last7_sum_started_at_hour"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

latest = (
    latest_match
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device",
        "started_at_hour",
        "last7_max_started_at_hour",
        "last7_min_started_at_hour",
        "last7_sum_started_at_hour",
        "duration_sec",
        "web_event_event_type",
    )
)

not_flagged = (
    latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < "2025-05-01"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "device",
        "started_at_hour",
        "last7_max_started_at_hour",
        "last7_min_started_at_hour",
        "last7_sum_started_at_hour",
        "duration_sec",
        "web_event_event_type",
    )
    .orderBy(F.col("last7_min_started_at_hour").desc(), F.col("device").desc())
)
