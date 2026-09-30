from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("humidity_sum_1h", F.sum("humidity").over(w))
    .withColumn("device_code_max_temp_c", F.max("temp_c").over(w2))
)
