from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") != "1.0.3")
        .select("firmware", "device_code")
    )
)
