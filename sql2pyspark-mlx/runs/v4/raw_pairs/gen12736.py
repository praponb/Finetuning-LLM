from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select("*", F.dayofmonth("reading_ts").alias("reading_ts_day"))
)

result = (
    valid
    .select(
        "*",
        F.avg("reading_ts_day").over(w).alias("reading_ts_day_avg_1h"),
        F.first("temp_c", ignorenulls=True).over(w2).alias("first_known_temp_c"),
    )
)
