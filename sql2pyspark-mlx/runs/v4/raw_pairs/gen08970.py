from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc())

latest = (
    df_device_readings
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "reading_ts", "temp_c", "pressure", "status_bits", "firmware")
    .orderBy(F.col("id").desc_nulls_first())
)
