from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .withColumn("status_bits_avg_3", F.avg("status_bits").over(w))
    .withColumn("pressure_bucket", F.ntile(10).over(w2))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w3))
)
