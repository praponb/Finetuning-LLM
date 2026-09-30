from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        F.last("temp_c", ignorenulls=True).over(w).alias("last_known_temp_c"),
        F.first("temp_c", ignorenulls=True).over(w2).alias("first_known_temp_c"),
        F.sum("humidity").over(w3).alias("device_code_sum_humidity"),
    )
)
