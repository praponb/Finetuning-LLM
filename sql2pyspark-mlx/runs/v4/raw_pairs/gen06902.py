from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "humidity", "pressure")
    .unpivot(["id", "device_code"], ["humidity", "pressure"], "measure", "value")
)
