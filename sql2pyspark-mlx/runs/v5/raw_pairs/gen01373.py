from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("humidity", "id")

valid = (
    df_device_readings
    .filter(F.col("pressure") > 1025)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

result = (
    valid
    .filter(F.col("humidity") <= 36)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "firmware",
        F.lead("status_bits").over(w_grp).alias("next_status_bits"),
        F.lag("status_bits").over(w_grp).alias("prev_status_bits"),
        F.lead("humidity").over(w_grp).alias("next_humidity"),
        F.lag("humidity").over(w_grp).alias("prev_humidity"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)
