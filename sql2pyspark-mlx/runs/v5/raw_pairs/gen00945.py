from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy("temp_c", "id")

with_windows = (
    df_device_readings
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w)), 4))
    .withColumn("device_code_avg_status_bits", F.avg("status_bits").over(w))
    .withColumn("next_humidity", F.lead("humidity", 1).over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_share",
    )
)
