from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("firmware", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c") < 38)
        .select("firmware", "id")
    )
)
