from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY_VAL"),
        F.col("temp_c").alias("TEMP_C_VAL"),
        F.col("pressure").alias("PRESSURE_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY_VAL", "TEMP_C_VAL", "PRESSURE_VAL"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
