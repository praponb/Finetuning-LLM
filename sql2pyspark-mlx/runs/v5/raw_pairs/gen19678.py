from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "pressure", "humidity", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "humidity", "temp_c"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
