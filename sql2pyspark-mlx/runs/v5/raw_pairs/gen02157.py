from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts_hour").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("is_new_burst", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts_hour", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .withColumn("reading_ts_hour", F.hour("reading_ts"))
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "reading_ts_hour",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

with_gaps = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "reading_ts_hour",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

flagged = (
    sessionized_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized_device_readings_2 = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

ranked = (
    sessionized_device_readings_2
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .filter(~F.col("device_code").isin(["DV-04", "DV-02"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "reading_ts_hour",
        "is_new_burst",
        "session_id",
        F.lead("session_id").over(w4).alias("next_session_id"),
        F.lag("island_id").over(w4).alias("prev_island_id"),
        F.lead("island_id").over(w4).alias("next_island_id"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("reading_ts_hour").over(w5).alias("reading_ts_hour_avg_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("reading_ts_hour", 0.95).alias("reading_ts_hour_p95"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
    .filter(F.col("avg_row_seq") >= 0)
    .orderBy(F.col("span_minutes").desc())
)
