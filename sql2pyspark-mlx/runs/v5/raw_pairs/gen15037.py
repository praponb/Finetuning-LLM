from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_event_type").orderBy(F.col("web_event_event_time").desc(), "id")

valid = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select("*", F.substring("device", 1, 4).alias("device_prefix"))
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.device_prefix",
        F.col("c.event_type"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.event_type",
        "s.web_event_event_time",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

with_windows = (
    with_latest
    .filter(F.col("web_event_event_type") == "click")
    .withColumn("event_seq", F.row_number().over(w2))
)

totals = (
    with_windows
    .groupBy(
        F.date_trunc("YEAR", "web_event_event_time").alias("year_start"),
        F.date_trunc("MONTH", "web_event_event_time").alias("activity_month"),
    )
    .agg(
        ((F.unix_timestamp(F.max("web_event_event_time")) - F.unix_timestamp(F.min("web_event_event_time"))) / 60).alias("duration_min"),
        F.max("event_seq").alias("max_event_seq"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

result = (
    totals
    .select("year_start", "activity_month", "max_event_seq", "distinct_event_types")
)
