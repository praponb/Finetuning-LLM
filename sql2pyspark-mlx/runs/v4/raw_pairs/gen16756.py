from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", F.col("pressure").alias("PRESSURE"), "temp_c")
    .unpivot(["id", "device_code"], ["PRESSURE", "temp_c"], "measure", "metric_value")
)
