from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("humidity").alias("HUMIDITY"),
        F.col("temp_c").alias("TEMP_C_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["PRESSURE_VAL", "HUMIDITY", "TEMP_C_VAL"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
