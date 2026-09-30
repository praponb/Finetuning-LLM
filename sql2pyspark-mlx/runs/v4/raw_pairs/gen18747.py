from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("hour_window").orderBy(F.col("avg_is_new_session").desc())

base = (
    df_device_readings
    .filter(F.col("firmware").isin(["1.0.3", "2.0.1", "1.1.0"]))
    .withColumn("status_bits_rounded", F.round("status_bits", 1))
    .withColumn("firmware_device_code", F.concat("firmware", F.lit(" "), "device_code"))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

flagged = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized_device_readings = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

filtered = (
    sessionized_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_rounded",
        "firmware_device_code",
        "session_id",
        "is_new_session",
        "session_no",
        F.greatest("pressure", "session_start_flag").alias("greatest_pressure_session_start_flag"),
    )
)

base_device_readings = (
    filtered
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select("*", F.upper("device_code").alias("device_code_upper"))
)

grouped = (
    base_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"), "device_code")
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        F.min("reading_ts").alias("started_at"),
    )
    .filter(F.col("avg_is_new_session") >= 0)
    .withColumn("avg_is_new_session_percent_rank", F.percent_rank().over(w3))
)

result = (
    grouped
    .select(
        "hour_window",
        "device_code",
        "avg_is_new_session",
        "distinct_firmwares",
        "corr_pressure_status_bits",
        "started_at",
        "avg_is_new_session_percent_rank",
    )
    .orderBy(F.col("corr_pressure_status_bits").desc_nulls_first())
)
