from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-10-15").cast("date")) & (F.col("event_time") < "2024-05-01"))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("duration_sec") >= 530, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
