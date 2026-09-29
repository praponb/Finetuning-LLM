from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("pressure", "id")

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "firmware",
        F.lead("temp_c").over(w_running).alias("next_temp_c"),
        F.lead("humidity").over(w_running).alias("next_humidity"),
        F.lag("humidity").over(w_running).alias("prev_humidity"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)
