from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("status_bits", "id")

result = (
    df_device_readings
    .select(
        "*",
        F.lag("temp_c", 2).over(w).alias("prev_temp_c"),
        F.last("humidity", ignorenulls=True).over(w2).alias("last_known_humidity"),
        F.min("temp_c").over(w3).alias("device_code_min_temp_c"),
        F.lag("pressure", 2).over(w4).alias("prev_pressure"),
        F.lag("pressure", 1).over(w5).alias("prev_pressure_2"),
    )
)
