from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy("pressure", "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.substring("firmware", 1, 4).alias("firmware_prefix"),
        (F.col("temp_c") * F.col("status_bits")).alias("temp_c_times_status_bits"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.sum("pressure").over(w).alias("pressure_sum_30m"),
        F.ntile(4).over(w2).alias("pressure_bucket"),
    )
)
