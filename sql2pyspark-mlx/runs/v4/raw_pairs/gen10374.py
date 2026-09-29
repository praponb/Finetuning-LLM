from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("humidity_avg_6_prior", F.avg("humidity").over(w))
    .withColumn("device_code_stddev_samp_pressure", F.stddev_samp("pressure").over(w2))
)
