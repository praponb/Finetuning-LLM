from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "temp_c", "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["temp_c", "pressure"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
