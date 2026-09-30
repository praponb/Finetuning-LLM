from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "*",
        F.corr("humidity", "pressure").over(w).alias("corr_humidity_pressure"),
        F.sum("temp_c").over(w2).alias("running_temp_c"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "corr_humidity_pressure",
    )
    .orderBy(F.col("firmware").desc())
)
