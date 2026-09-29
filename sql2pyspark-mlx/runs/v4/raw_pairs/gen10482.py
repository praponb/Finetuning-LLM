from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "*",
        F.lag("temp_c", 1, 0).over(w).alias("prev_temp_c"),
        F.sum("status_bits").over(w2).alias("status_bits_sum_2h"),
        F.sum("pressure").over(w3).alias("running_pressure"),
        F.avg("pressure").over(w2).alias("pressure_avg_2h"),
    )
)
