from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("device_code_length").desc(), "id")
w4 = Window.orderBy("device_code_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("pressure") <= 995)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.floor("humidity").alias("humidity_whole"),
        F.length("device_code").alias("device_code_length"),
    )
)

result = (
    cleaned
    .filter(F.col("humidity").between(47, 71))
    .select(
        "id",
        "temp_c",
        "humidity",
        "humidity_whole",
        "device_code_length",
        F.first("humidity", ignorenulls=True).over(w).alias("first_known_humidity"),
        F.sum("device_code_length").over(w2).alias("running_device_code_length"),
        F.first("humidity", ignorenulls=True).over(w).alias("first_known_humidity_2"),
        F.ntile(5).over(w3).alias("device_code_length_bucket"),
        F.first("pressure", ignorenulls=True).over(w4).alias("first_known_pressure"),
    )
)
