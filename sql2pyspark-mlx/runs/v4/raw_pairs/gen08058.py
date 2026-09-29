from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code")

with_windows = (
    df_device_readings
    .withColumn("humidity_avg_3", F.avg("humidity").over(w))
    .withColumn("device_code_stddev_samp_pressure", F.stddev_samp("pressure").over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "device_code_stddev_samp_pressure",
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(20)
)
