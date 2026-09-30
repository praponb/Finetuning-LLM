from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .withColumn("humidity_sum_30m", F.sum("humidity").over(w_recent))
    .withColumn("device_code_avg_humidity", F.avg("humidity").over(w2))
)
