from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("status_bits") <= 4))
    .withColumn("pressure_filled", F.coalesce("pressure", F.lit(0)))
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 76, "LARGE").otherwise("SMALL"))
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .groupingSets(
        [["humidity_tier", "device_code", "hour_window"], ["humidity_tier", "device_code"], []],
        "humidity_tier",
        "device_code",
        "hour_window",
    )
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        F.grouping("humidity_tier").alias("gid"),
    )
    .filter(F.col("session_start_flag_p95") < 0)
    .orderBy(F.col("session_start_flag_p95").desc())
)
