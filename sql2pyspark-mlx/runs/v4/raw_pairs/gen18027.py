from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")

latest = (
    df_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select("id", "device_code", "temp_c", "pressure", "firmware")
    .orderBy(F.col("pressure").desc_nulls_last(), F.col("temp_c").desc())
)
