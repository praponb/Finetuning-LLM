from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("humidity") > 47)
        .select("device_code", "id")
    )
)
