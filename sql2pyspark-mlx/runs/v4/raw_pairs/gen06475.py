from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("temp_c").between(22, 34))
        .select("firmware", "device_code")
    )
)
