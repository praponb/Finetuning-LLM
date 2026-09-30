from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first())
w2 = Window.orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("pressure_cume_dist", F.cume_dist().over(w))
    .withColumn("prev_status_bits", F.lag("status_bits", 1, 0).over(w2))
    .withColumn("device_code_avg_humidity", F.avg("humidity").over(w3))
)
