from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy("status_bits", "id")

base = (
    df_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "*",
        F.lower("device_code").alias("device_code_lower"),
        F.concat("firmware", F.lit(", "), "device_code").alias("firmware_device_code"),
    )
)

prev_events = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

scored = (
    islands
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_device_code",
        "session_start_flag",
        "burst_id",
        F.corr("temp_c", "pressure").over(w3).alias("corr_temp_c_pressure"),
        F.ntile(10).over(w4).alias("status_bits_bucket"),
    )
)

remaining_keys = (
    scored
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("device_code") == "DV-02")
        .select("device_code", "firmware")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 13, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["status_bits_tier", "burst_id"], ["status_bits_tier"]],
        "status_bits_tier",
        "burst_id",
    )
    .agg(
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.round(F.sum("corr_temp_c_pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("corr_temp_c_pressure_per_dv_01"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("corr_temp_c_pressure_per_dv_01") > 0)
    .orderBy(F.col("burst_id").desc())
)
