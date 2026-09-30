from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy("status_bits_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w_ordered = Window.partitionBy("device_code").orderBy("corr_humidity_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("running_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w8 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("status_bits_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 10:00:00").cast("timestamp")) & (F.col("humidity") < 38))
    .select("*", F.floor("status_bits").alias("status_bits_whole"))
)

scored = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("temp_c_cume_dist"),
        F.sum("pressure").over(w2).alias("running_pressure"),
        F.corr("humidity", "pressure").over(w3).alias("corr_humidity_pressure"),
    )
)

scored_device_readings = (
    scored
    .select(
        "*",
        F.sum("corr_humidity_pressure").over(w_ordered).alias("cumulative_corr_humidity_pressure"),
        F.first("pressure", ignorenulls=True).over(w5).alias("first_known_pressure"),
        F.sum("temp_c").over(w_part).alias("temp_c_sum_30m"),
    )
)

flagged = (
    scored_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w7).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w7) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_id", F.sum("session_start_flag").over(w8))

with_windows = (
    sessions_device_readings
    .select(
        "*",
        F.last("cumulative_corr_humidity_pressure", ignorenulls=True).over(w9).alias("last_known_cumulative_corr_humidity_pressure"),
        F.first("temp_c_sum_30m", ignorenulls=True).over(w8).alias("first_known_temp_c_sum_30m"),
        F.ntile(3).over(w10).alias("status_bits_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
    .withColumn("abs_last_known_cumulative_corr_humidity_pressure", F.abs("last_known_cumulative_corr_humidity_pressure"))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_whole",
        "temp_c_cume_dist",
    )
    .orderBy("pressure")
)
