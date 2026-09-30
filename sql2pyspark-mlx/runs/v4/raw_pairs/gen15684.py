from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("firmware_length").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(12, 20))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.length("firmware").alias("firmware_length"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.avg("humidity").over(w).alias("humidity_avg_6"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
