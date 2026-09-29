from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("pressure", "id")
w2 = Window.orderBy(F.col("pressure").desc(), "id")

base = (
    df_device_readings
    .filter(F.col("pressure") < 1025)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.floor("status_bits").alias("status_bits_whole"),
    )
)

result = (
    base
    .filter(F.col("reading_ts") < "2023-07-01")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "status_bits_whole",
        F.lag("status_bits_whole").over(w_hist).alias("prev_status_bits_whole"),
        F.lead("status_bits_whole").over(w_hist).alias("next_status_bits_whole"),
        F.lag("humidity").over(w_hist).alias("prev_humidity"),
        F.lead("humidity").over(w_hist).alias("next_humidity"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
