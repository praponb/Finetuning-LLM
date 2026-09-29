from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "*",
        F.avg("status_bits").over(w).alias("status_bits_avg_5_prior"),
        F.corr("humidity", "pressure").over(w_grp).alias("corr_humidity_pressure"),
        F.corr("temp_c", "pressure").over(w_grp).alias("corr_temp_c_pressure"),
    )
)
