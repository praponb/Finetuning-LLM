from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

with_windows = (
    df_device_readings
    .withColumn("first_known_humidity", F.first("humidity", ignorenulls=True).over(w))
    .withColumn("humidity_avg_30m", F.avg("humidity").over(w2))
    .withColumn("pressure_count_1h", F.count("pressure").over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "first_known_humidity",
        "humidity_avg_30m",
        "pressure_count_1h",
    )
    .orderBy(F.col("pressure_count_1h").desc_nulls_last(), "humidity_avg_30m")
)
