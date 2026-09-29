from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.concat_ws(",", "firmware", "device_code").alias("firmware_device_code_key"),
    )
)

filtered_device_readings = (
    filtered
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("pressure").between(1007, 1012))
    .withColumn("pressure_minus_temp_c", (F.col("pressure") - F.col("temp_c")))
)

with_windows = (
    filtered_device_readings
    .filter(F.col("temp_c") <= 26)
    .withColumn("cumulative_pressure_minus_temp_c", F.sum("pressure_minus_temp_c").over(w_running))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w_running))
)

matched_keys = (
    with_windows
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") >= "2024-07-01")
        .select("device_code")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("pressure")).otherwise(0)).alias("dv_03_pressure"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.round(F.avg("cumulative_pressure_minus_temp_c"), 2).alias("avg_cumulative_pressure_minus_temp_c"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
