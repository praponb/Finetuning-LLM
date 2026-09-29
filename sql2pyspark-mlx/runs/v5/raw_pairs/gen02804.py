from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        (F.col("pressure") - F.col("status_bits")).alias("pressure_minus_status_bits"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "pressure_minus_status_bits",
        F.avg("temp_c").over(w).alias("temp_c_avg_5"),
        F.lag("status_bits", 1).over(w2).alias("prev_status_bits"),
        F.last("temp_c", ignorenulls=True).over(w3).alias("last_known_temp_c"),
    )
)

filtered_device_readings = (
    with_windows
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .withColumn("prev_status_bits_plus_pressure_minus_status_bits", (F.col("prev_status_bits") + F.col("pressure_minus_status_bits")))
)

remaining_keys = (
    filtered_device_readings
    .select("id", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("firmware").isin(["1.0.3", "2.0.1", "1.1.0"]))
        .select("id", "device_code")
    )
)

reconciled = (
    filtered_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "device_code",
        "reading_ts",
        "pressure",
        "pressure_minus_status_bits",
        "temp_c_avg_5",
        "prev_status_bits",
        "prev_status_bits_plus_pressure_minus_status_bits",
    )
    .orderBy(
        F.col("prev_status_bits_plus_pressure_minus_status_bits").desc_nulls_first(),
    )
)
