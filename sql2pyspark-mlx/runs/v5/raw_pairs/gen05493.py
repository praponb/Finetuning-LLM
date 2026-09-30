from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("humidity", "id")

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        F.lag("pressure").over(w_recent).alias("prev_pressure"),
        F.lead("pressure").over(w_recent).alias("next_pressure"),
        F.lag("humidity").over(w_recent).alias("prev_humidity"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)
