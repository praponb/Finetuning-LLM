from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(6, 11))
    .withColumn("temp_c_rounded", F.round("temp_c", 2))
)

with_prev = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

enriched = (
    sessions_device_readings
    .select(
        "*",
        F.count("session_no").over(w_ordered).alias("running_count_session_no"),
        F.avg("session_no").over(w_ordered).alias("running_avg_session_no"),
        F.min("session_no").over(w_ordered).alias("running_min_session_no"),
    )
)

prev_events = enriched.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "session_no",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

filtered = (
    sessionized
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 07:00:00"))) & (F.col("session_no") >= 4))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "session_no",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
        F.when(F.col("session_id") >= 7, "High").when(F.col("session_id") >= 3, "Medium").otherwise("Low").alias("session_id_level"),
    )
)

result = (
    filtered
    .select(
        "id",
        "reading_ts",
        "humidity",
        "session_no",
        "reading_ts_day",
        "session_id_level",
    )
    .orderBy(F.col("humidity").desc())
)
