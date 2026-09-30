from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy("status_bits")
w_ordered = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.dense_rank().over(w).alias("humidity_dense_rank"),
        F.percent_rank().over(w2).alias("status_bits_percent_rank"),
        F.dense_rank().over(w_ordered).alias("status_bits_dense_rank"),
        F.avg("pressure").over(w4).alias("pressure_avg_2"),
    )
)
