from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("is_dv_01", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("is_dv_01").desc(), "id")
w4 = Window.partitionBy("device_code")
w5 = Window.orderBy(F.col("humidity_minus_pressure_avg_2h").desc_nulls_first())
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 01:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        (F.col("humidity") - F.col("pressure")).alias("humidity_minus_pressure"),
        F.substring("device_code", 1, 3).alias("device_code_prefix"),
    )
)

cleaned = (
    base
    .filter(F.col("firmware") == "2.0.1")
    .withColumn("is_dv_01", F.when(F.col("device_code") == "DV-01", 1).otherwise(0))
)

base_device_readings = (
    cleaned
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 04:00:00")))
    .select(
        "*",
        (F.col("humidity_minus_pressure") * F.col("humidity")).alias("humidity_minus_pressure_times_humidity"),
    )
)

with_windows = (
    base_device_readings
    .select(
        "*",
        F.lag("humidity_minus_pressure", 2).over(w).alias("prev_humidity_minus_pressure"),
        F.avg("humidity_minus_pressure").over(w2).alias("humidity_minus_pressure_avg_2h"),
        F.ntile(5).over(w3).alias("is_dv_01_bucket"),
        F.min("humidity_minus_pressure_times_humidity").over(w4).alias("device_code_min_humidity_minus_pressure_times_humidity"),
    )
)

scored = (
    with_windows
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0", "1.0.3"]))
    .select(
        "*",
        F.cume_dist().over(w5).alias("humidity_minus_pressure_avg_2h_cume_dist"),
        F.max("is_dv_01").over(w6).alias("is_dv_01_max_1h"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("humidity_minus_pressure_times_humidity") >= 65, "PREMIUM").otherwise("STANDARD").alias("humidity_minus_pressure_times_humidity_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.max("reading_ts").alias("last_seen"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows_2"),
        F.min("reading_ts").alias("first_seen"),
    )
    .filter(F.col("dv_02_rows_2") > 1)
    .orderBy(F.col("dv_02_rows").desc_nulls_last())
)
