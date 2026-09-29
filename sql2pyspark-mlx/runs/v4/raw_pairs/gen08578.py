from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy(F.col("status_bits").desc(), "id")

base = (
    df_device_readings
    .filter(F.col("status_bits") <= 9)
    .withColumn("pressure_times_status_bits", (F.col("pressure") * F.col("status_bits")))
)

result = (
    base
    .withColumn("device_code_min_pressure", F.min("pressure").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
