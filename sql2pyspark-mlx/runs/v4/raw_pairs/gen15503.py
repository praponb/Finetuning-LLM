from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code", "temp_c", "pressure", "humidity")
    .unpivot(
        ["id", "device_code"],
        ["temp_c", "pressure", "humidity"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
