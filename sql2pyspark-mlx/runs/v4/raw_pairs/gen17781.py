from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())

result = (
    df_device_readings
    .withColumn("humidity_max_1h", F.max("humidity").over(w))
    .withColumn("device_code_avg_humidity", F.avg("humidity").over(w2))
    .withColumn("status_bits_dense_rank", F.dense_rank().over(w3))
)
