from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w_running = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "*",
        F.lead("temp_c", 1).over(w).alias("next_temp_c"),
        F.ntile(3).over(w_running).alias("pressure_bucket"),
        F.sum("temp_c").over(w3).alias("cumulative_temp_c"),
        F.first("pressure", ignorenulls=True).over(w4).alias("first_known_pressure"),
        F.min("status_bits").over(w5).alias("device_code_min_status_bits"),
    )
)
