from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last())
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")

with_windows = (
    df_device_readings
    .select(
        "*",
        F.avg("temp_c").over(w).alias("temp_c_avg_30m"),
        F.percent_rank().over(w2).alias("temp_c_percent_rank"),
        F.ntile(10).over(w3).alias("pressure_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_avg_30m",
        "temp_c_percent_rank",
    )
    .orderBy(F.col("reading_ts").asc_nulls_last())
)
