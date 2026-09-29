from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("started_at", "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & ~F.col("country").isin(["DE", "SG"]))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

prev_events = islands.withColumn("prev_started_at", F.lag("started_at").over(w3))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w4))

latest_match = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

result = (
    latest_match
    .groupBy("browser", "device")
    .pivot("country", ["US", "JP"])
    .agg(F.avg("country_length").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
