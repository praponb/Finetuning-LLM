from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_per_add_to_cart").desc())

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 454, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("duration_sec_per_add_to_cart"),
        F.percentile_approx("island_id", 0.5).alias("island_id_p50"),
    )
    .withColumn("duration_sec_per_add_to_cart_dense_rank", F.dense_rank().over(w3))
)
