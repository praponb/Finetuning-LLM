from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select("id", "reading_ts", "temp_c", "humidity", "pressure", "firmware", "row_num")
    .orderBy(F.col("temp_c").asc_nulls_first())
)
