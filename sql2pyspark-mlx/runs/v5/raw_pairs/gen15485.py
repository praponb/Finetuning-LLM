from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.upper("firmware").alias("firmware_upper"))
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 12, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_day", "status_bits_tier"], ["activity_day"], []],
        "activity_day",
        "status_bits_tier",
    )
    .agg(
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("temp_c")).otherwise(0)).alias("dv_01_temp_c"),
    )
    .orderBy(F.col("status_bits_tier").desc())
)
