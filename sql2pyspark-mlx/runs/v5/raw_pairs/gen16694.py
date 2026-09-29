from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code", "firmware")
    .intersect(
        df_device_readings
        .filter(F.col("device_code") == "DV-01")
        .select("device_code", "firmware")
    )
)
