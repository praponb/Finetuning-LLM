from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("firmware_lower", F.lower("firmware"))
)

remaining_keys = (
    cleaned
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("pressure") > 1016)
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.avg("humidity").over(w).alias("humidity_avg_30m"),
        F.avg("temp_c").over(w2).alias("temp_c_avg_1h"),
    )
)

result = (
    metrics
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.round(F.avg("humidity_avg_30m"), 2).alias("avg_humidity_avg_30m"),
        F.max("pressure").alias("max_pressure"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("hour_window").desc())
)
