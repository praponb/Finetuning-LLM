from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at_hour", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id", "device").orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("device")
w5 = Window.orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("started_at", "id")
w7 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-05-22").cast("date"))
    .withColumn("started_at_hour", F.hour("started_at"))
    .withColumn("device_length", F.length("device"))
)

metrics = (
    base
    .withColumn("started_at_hour_avg_2", F.avg("started_at_hour").over(w))
    .withColumn("cumulative_started_at_hour", F.sum("started_at_hour").over(w2))
    .withColumn("customer_id_count_started_at_hour", F.count("started_at_hour").over(w3))
    .withColumn("device_avg_started_at_hour", F.avg("started_at_hour").over(w4))
    .withColumn("first_known_started_at_hour", F.first("started_at_hour", ignorenulls=True).over(w5))
)

with_gaps = (
    metrics
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w6).isNull() | (F.col("started_at") > F.lag("started_at").over(w6) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w7))

eligible = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "started_at",
        "cumulative_started_at_hour",
        "device_avg_started_at_hour",
        "first_known_started_at_hour",
        "island_id",
    )
    .orderBy(F.col("cumulative_started_at_hour").asc_nulls_first(), "started_at")
)
