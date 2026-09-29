from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 02:00:00").cast("timestamp")) & ~F.col("device_code").isin(["DV-04", "DV-02", "DV-01"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.col("humidity").cast("int").alias("humidity_int"),
        F.hour("reading_ts").alias("reading_ts_hour"),
    )
)

remaining_keys = (
    valid
    .select("firmware")
    .subtract(
        df_device_readings
        .filter(F.col("device_code") == "DV-02")
        .select("firmware")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "device_code",
        "humidity",
        "pressure",
        "firmware",
        "reading_ts_hour",
        F.corr("pressure", "humidity").over(w).alias("corr_pressure_humidity"),
    )
)

valid_device_readings = (
    with_windows
    .filter(F.col("humidity").between(51, 63))
    .withColumn("firmware_device_code", F.concat("firmware", F.lit(" - "), "device_code"))
)

scored = (
    valid_device_readings
    .select(
        "firmware",
        "reading_ts_hour",
        "corr_pressure_humidity",
        F.avg("reading_ts_hour").over(w).alias("device_code_avg_reading_ts_hour"),
        F.corr("humidity", "pressure").over(w).alias("corr_humidity_pressure"),
    )
)

result = scored.select("firmware", "reading_ts_hour")
