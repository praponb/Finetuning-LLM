from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure_times_humidity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("pressure_times_humidity", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 09:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("pressure") * F.col("humidity")).alias("pressure_times_humidity"),
    )
)

valid = (
    filtered
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        "pressure_times_humidity",
        F.substring("firmware", 1, 2).alias("firmware_prefix"),
        F.trim("firmware").alias("clean_firmware"),
    )
)

scored = (
    valid
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "clean_firmware",
        F.count("pressure_times_humidity").over(w).alias("last4_count_pressure_times_humidity"),
        F.max("pressure_times_humidity").over(w).alias("last4_max_pressure_times_humidity"),
        F.min("pressure_times_humidity").over(w).alias("last4_min_pressure_times_humidity"),
        F.avg("pressure_times_humidity").over(w2).alias("pressure_times_humidity_avg_2_prior"),
    )
)

prev_events = scored.withColumn("prev_reading_ts", F.lag("reading_ts").over(w3))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w4))

daily = (
    sessionized
    .withColumn("last4_min_pressure_times_humidity_tier", F.when(F.col("last4_min_pressure_times_humidity") >= 80, "PREMIUM").otherwise("STANDARD"))
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .groupingSets(
        [["last4_min_pressure_times_humidity_tier", "hour_window"], ["last4_min_pressure_times_humidity_tier"], []],
        "last4_min_pressure_times_humidity_tier",
        "hour_window",
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.percentile_approx("pressure_times_humidity_avg_2_prior", [0.25, 0.5, 0.75]).alias("pressure_times_humidity_avg_2_prior_quartiles"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.min("prev_reading_ts").alias("first_prev_reading_ts"),
    )
    .filter(F.col("dv_02_rows") < 1000)
)

result = (
    daily
    .select(
        "last4_min_pressure_times_humidity_tier",
        "hour_window",
        "dv_04_events",
        "dv_02_rows",
        "pressure_times_humidity_avg_2_prior_quartiles",
        "dv_02_events",
        "first_prev_reading_ts",
    )
    .orderBy(F.col("first_prev_reading_ts").desc())
)
