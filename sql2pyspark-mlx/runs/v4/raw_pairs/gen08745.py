from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_device_readings
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w))
    .withColumn("running_pressure", F.sum("pressure").over(w2))
    .withColumn("running_humidity", F.sum("humidity").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "corr_status_bits_humidity",
        "running_pressure",
    )
    .orderBy(F.col("corr_status_bits_humidity").desc(), F.col("humidity").desc())
)
