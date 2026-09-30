from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")
w2 = Window.orderBy(F.col("temp_c").desc())
w3 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "*",
        F.lag("status_bits", 1).over(w).alias("prev_status_bits"),
        F.dense_rank().over(w2).alias("temp_c_dense_rank"),
        F.min("humidity").over(w3).alias("device_code_min_humidity"),
    )
)
