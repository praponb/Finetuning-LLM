from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c_int").desc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits_times_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c_sum_2h").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("pressure").between(996, 1023))
    .select(
        "*",
        F.col("temp_c").cast("int").alias("temp_c_int"),
        (F.col("status_bits") * F.col("humidity")).alias("status_bits_times_humidity"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .filter(F.col("pressure").between(1010, 1027))
    .withColumn("status_bits_max_15m", F.max("status_bits").over(w2))
    .withColumn("running_temp_c", F.sum("temp_c").over(w3))
    .withColumn("temp_c_sum_2h", F.sum("temp_c").over(w4))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_int",
        "status_bits_times_humidity",
        "row_num",
    )
)
