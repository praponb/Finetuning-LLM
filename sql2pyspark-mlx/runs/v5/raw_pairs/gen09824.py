from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.orderBy(F.col("humidity").desc_nulls_first())
w4 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_device_readings
    .withColumn("prev_temp_c", F.lag("temp_c", 1).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("humidity_rank", F.rank().over(w3))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w4))
    .withColumn("last_known_temp_c", F.last("temp_c", ignorenulls=True).over(w5))
)

result = (
    scored
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_temp_c",
        "last_known_temp_c",
    )
    .orderBy("humidity")
)
