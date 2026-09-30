from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w))
    .withColumn("corr_temp_c_status_bits", F.corr("temp_c", "status_bits").over(w2))
)
