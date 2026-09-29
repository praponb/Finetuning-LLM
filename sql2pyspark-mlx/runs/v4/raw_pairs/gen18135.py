from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w_grp = Window.orderBy("temp_c", "id")
w_part = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("temp_c_bucket", F.ntile(5).over(w_grp))
    .withColumn("status_bits_bucket", F.ntile(4).over(w_part))
    .withColumn("running_status_bits", F.sum("status_bits").over(w4))
    .withColumn("device_code_min_status_bits", F.min("status_bits").over(w5))
)
