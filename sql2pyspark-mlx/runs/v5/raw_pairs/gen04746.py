from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-08-09").cast("date")) & F.col("page").isNotNull())
    .withColumn("page_upper", F.upper("page"))
    .withColumn("page_filled", F.coalesce("page", F.lit("DIRECT")))
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("session_id")
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
    )
    .filter(F.col("duration_sec_bit_or") >= 1)
    .orderBy("session_id")
)
