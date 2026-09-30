from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("pressure", "id").rowsBetween(-5, -1)
w_grp = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("temp_c", "id")
w4 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last_known_humidity", "id").rowsBetween(-3, -1)
w_running = Window.orderBy("temp_c", "id").rowsBetween(-3, -1)
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.orderBy("prev_temp_c_times_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("humidity").between(44, 68))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        (F.col("temp_c") * F.col("humidity")).alias("temp_c_times_humidity"),
    )
)

with_windows = (
    filtered
    .withColumn("pressure_avg_5_prior", F.avg("pressure").over(w_hist))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w_grp))
    .withColumn("event_seq", F.row_number().over(w3))
)

metrics = (
    with_windows
    .withColumn("running_temp_c", F.sum("temp_c").over(w4))
    .withColumn("cumulative_temp_c", F.sum("temp_c").over(w4))
)

with_windows_device_readings = (
    metrics
    .withColumn("prev_temp_c_times_humidity", F.lag("temp_c_times_humidity", 1).over(w3))
    .withColumn("running_temp_c_avg_3_prior", F.avg("running_temp_c").over(w5))
)

metrics_device_readings = (
    with_windows_device_readings
    .select(
        "*",
        F.avg("last_known_humidity").over(w_running).alias("last_known_humidity_avg_3_prior"),
        F.avg("cumulative_temp_c").over(w7).alias("cumulative_temp_c_avg_30m"),
        F.last("pressure_avg_5_prior", ignorenulls=True).over(w8).alias("last_known_pressure_avg_5_prior"),
        F.first("cumulative_temp_c", ignorenulls=True).over(w9).alias("first_known_cumulative_temp_c"),
    )
)

result = (
    metrics_device_readings
    .withColumn("temp_c_times_humidity_tier", F.when(F.col("temp_c_times_humidity") >= 61, "PREMIUM").otherwise("STANDARD"))
    .withColumn("first_known_cumulative_temp_c_tier", F.when(F.col("first_known_cumulative_temp_c") >= 171, "LARGE").otherwise("SMALL"))
    .rollup("temp_c_times_humidity_tier", "first_known_cumulative_temp_c_tier")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
        F.avg("cumulative_temp_c_avg_30m").alias("avg_cumulative_temp_c_avg_30m"),
        F.grouping("temp_c_times_humidity_tier").alias("gid"),
    )
)
