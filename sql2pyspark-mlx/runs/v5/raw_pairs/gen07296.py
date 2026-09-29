from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "humidity",
        F.corr("status_bits", "pressure").over(w).alias("corr_status_bits_pressure"),
        F.corr("status_bits", "pressure").over(w).alias("corr_status_bits_pressure_2"),
        F.last("temp_c", ignorenulls=True).over(w2).alias("last_known_temp_c"),
        F.avg("temp_c").over(w3).alias("temp_c_avg_3"),
        F.cume_dist().over(w4).alias("humidity_cume_dist"),
    )
)
