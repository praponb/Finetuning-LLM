from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id")

result = (
    df_device_readings
    .withColumn("status_bits_rank", F.rank().over(w))
    .withColumn("prev_status_bits", F.lag("status_bits", 1).over(w2))
    .withColumn("status_bits_bucket", F.ntile(10).over(w3))
)
