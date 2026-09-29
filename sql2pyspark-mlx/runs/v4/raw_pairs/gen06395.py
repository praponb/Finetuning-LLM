from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code", "id")
    .intersect(
        df_device_readings
        .filter(F.col("humidity").isNotNull())
        .select("device_code", "id")
    )
)
