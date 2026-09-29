from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("device_code") != "DV-02")
    .select(
        "*",
        (F.col("pressure") + F.col("humidity")).alias("pressure_plus_humidity"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("humidity").over(w2).alias("running_humidity"),
    )
)
