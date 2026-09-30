from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.col("reading_ts") >= "2024-05-08")
        .select("device_code", "firmware")
    )
)
