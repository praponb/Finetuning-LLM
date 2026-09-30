from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .withColumn("pressure_minus_humidity", (F.col("pressure") - F.col("humidity")))
)

result = (
    cleaned
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") != "1.0.3")
        .select("device_code")
    )
)
