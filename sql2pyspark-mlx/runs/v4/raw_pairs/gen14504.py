from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "pressure", "temp_c")
    .unpivot(["id", "device_code"], ["pressure", "temp_c"], "kpi", "value")
)
