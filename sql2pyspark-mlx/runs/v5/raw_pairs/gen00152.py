from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy("started_at_hour", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("device", "browser").orderBy("started_at", "id")
w4 = Window.orderBy(F.col("started_at_hour_count_30d").asc_nulls_first())
w5 = Window.partitionBy("customer_id").orderBy("prev_started_at_hour", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("customer_id").orderBy("started_at_hour_avg_2", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(~F.col("browser").eqNullSafe(F.lit("Chrome")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.hour("started_at").alias("started_at_hour"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "started_at_hour",
        F.count("started_at_hour").over(w).alias("started_at_hour_count_30d"),
        F.avg("started_at_hour").over(w2).alias("started_at_hour_avg_2"),
        F.lag("started_at_hour", 1).over(w_recent).alias("prev_started_at_hour"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "started_at_hour",
        "prev_started_at_hour",
        F.cume_dist().over(w4).alias("started_at_hour_count_30d_cume_dist"),
        F.avg("started_at_hour_avg_2").over(w5).alias("started_at_hour_avg_2_avg_6_prior"),
        F.avg("started_at_hour").over(w6).alias("started_at_hour_avg_6_prior"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.started_at_hour",
        "s.prev_started_at_hour",
        "s.started_at_hour_count_30d_cume_dist",
        "s.started_at_hour_avg_2_avg_6_prior",
        F.col("c.page"),
        F.col("c.event_type"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "started_at_hour",
        "prev_started_at_hour",
        "started_at_hour_count_30d_cume_dist",
        "started_at_hour_avg_2_avg_6_prior",
        "page",
    )
)
