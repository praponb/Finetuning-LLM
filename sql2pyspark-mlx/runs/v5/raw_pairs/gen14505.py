from pyspark.sql import functions as F

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "pressure",
        F.col("humidity").alias("HUMIDITY_VAL"),
        "temp_c",
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "HUMIDITY_VAL", "temp_c"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
