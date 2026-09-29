from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w_grp = Window.partitionBy("browser").orderBy("started_at", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

with_windows = (
    not_flagged
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

base = (
    with_windows
    .filter(F.col("customer_id").isNotNull() & (~F.col("country").eqNullSafe(F.lit("DE"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "event_seq",
        "row_seq",
        "seq_no",
        F.length("device").alias("device_length"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.started_at",
        "s.country",
        "s.event_seq",
        "s.row_seq",
        "s.device_length",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

prev_events = with_last.withColumn("prev_started_at", F.lag("started_at").over(w_grp))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "browser",
        "started_at",
        "row_seq",
        "device_length",
        F.sum("session_start_flag").over(w5).alias("island_id"),
    )
)

with_latest = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.started_at", "s.device_length", F.col("c.event_time"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("device_length") >= 80, "HIGH").otherwise("LOW").alias("device_length_tier"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("device_length", [0.25, 0.5, 0.75]).alias("device_length_quartiles"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("event_time").alias("ended_at"),
    )
)
