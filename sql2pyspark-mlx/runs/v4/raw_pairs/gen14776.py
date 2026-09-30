from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id")

result = (
    df_device_readings
    .withColumn("status_bits_avg_3", F.avg("status_bits").over(w))
    .withColumn("corr_status_bits_pressure", F.corr("status_bits", "pressure").over(w2))
    .withColumn("prev_humidity", F.lag("humidity", 2, 0).over(w3))
)
