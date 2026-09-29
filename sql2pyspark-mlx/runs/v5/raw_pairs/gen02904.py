from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code", "id")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") > 9)
        .select("device_code", "id")
    )
)
