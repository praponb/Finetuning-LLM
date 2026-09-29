from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "pressure", "temp_c", "humidity")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "temp_c", "humidity"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
