from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id")
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")

metrics = (
    df_device_readings
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w))
    .withColumn("running_temp_c", F.sum("temp_c").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "last_known_humidity",
        "running_temp_c",
    )
    .orderBy(F.col("humidity").desc_nulls_last())
)
