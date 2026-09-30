from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        "humidity",
        F.col("temp_c").alias("TEMP_C_VAL"),
    )
    .unpivot(
        ["id", "device_code"],
        ["PRESSURE_VAL", "humidity", "TEMP_C_VAL"],
        "measure",
        "val",
    )
)
