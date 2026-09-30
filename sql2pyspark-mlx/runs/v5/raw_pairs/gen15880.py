from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")

result = (
    df_device_readings
    .filter(F.col("temp_c") < 37)
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4).alias("temp_c_share"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
