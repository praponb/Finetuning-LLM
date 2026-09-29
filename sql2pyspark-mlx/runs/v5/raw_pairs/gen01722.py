from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits_bucket").desc())
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w5 = Window.partitionBy("device_code")

scored = (
    df_device_readings
    .filter(F.col("temp_c") <= 31)
    .select(
        "*",
        F.last("humidity", ignorenulls=True).over(w).alias("last_known_humidity"),
        F.ntile(3).over(w2).alias("status_bits_bucket"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

deduped_device_readings = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_device_readings_2 = (
    deduped_device_readings
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_device_readings_2
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "last_known_humidity",
        "status_bits_bucket",
        "latest_rank",
        F.round((F.col("last_known_humidity") / F.sum("last_known_humidity").over(w5)), 4).alias("last_known_humidity_share"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w5)), 4).alias("temp_c_share"),
    )
)
