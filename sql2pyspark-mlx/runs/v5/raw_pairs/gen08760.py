from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 01:00:00").cast("timestamp")) & (F.col("status_bits") <= 9))
    .withColumn("pressure_int", F.col("pressure").cast("int"))
)

result = (
    valid
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "pressure_int",
        F.lead("temp_c").over(w_ordered).alias("next_temp_c"),
        F.lag("temp_c").over(w_ordered).alias("prev_temp_c"),
        F.lead("status_bits").over(w_ordered).alias("next_status_bits"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)
