from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())
w_recent = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_bits", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last())

result = (
    df_device_readings
    .select(
        "*",
        F.count("temp_c").over(w_running).alias("device_code_count_temp_c"),
        F.dense_rank().over(w2).alias("humidity_dense_rank"),
        F.first("humidity", ignorenulls=True).over(w_recent).alias("first_known_humidity"),
        F.lead("temp_c", 1).over(w4).alias("next_temp_c"),
        F.dense_rank().over(w5).alias("temp_c_dense_rank"),
    )
)
