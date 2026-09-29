from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id_2_tier").orderBy(F.col("duration_min").desc())

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/search"))) & F.col("session_id").isin([3, 10, 15, 22]))
    .withColumn("event_type_page_key", F.concat_ws("/", "event_type", "page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "page",
        "event_time",
        "event_type_page_key",
        F.sum("session_start_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("session_id_2") >= 9, "HIGH").otherwise("LOW").alias("session_id_2_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.max("event_time").alias("ended_at"),
        F.min("event_time").alias("first_seen"),
        F.percentile_approx("session_id_2", [0.25, 0.5, 0.75]).alias("session_id_2_quartiles"),
    )
    .filter(F.col("duration_min") >= 0)
    .withColumn("duration_min_percent_rank", F.percent_rank().over(w3))
)
