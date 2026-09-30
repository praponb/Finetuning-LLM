from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "temp_c", "pressure", "humidity")
    .unpivot(
        ids=["id", "device_code"],
        values=["temp_c", "pressure", "humidity"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
