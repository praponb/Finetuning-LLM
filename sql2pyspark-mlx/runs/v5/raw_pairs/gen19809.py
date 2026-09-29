from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("humidity").desc(), "id")
w_ordered = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w_running = Window.partitionBy("device_code").orderBy("temp_c", "id")

filtered = (
    df_device_readings
    .filter(F.col("pressure").between(998, 1027))
    .select("*", F.upper("firmware").alias("firmware_upper"))
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w_part).alias("seq_no"),
        F.ntile(5).over(w_ordered).alias("status_bits_bucket"),
        F.row_number().over(w_running).alias("seq_no_2"),
    )
)
