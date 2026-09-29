from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "pressure", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "temp_c"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
