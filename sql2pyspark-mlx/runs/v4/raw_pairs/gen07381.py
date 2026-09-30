from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("is_new_burst", "id")
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

base = (
    sessions_device_readings
    .filter(F.col("is_new_burst") <= 1)
    .withColumn("firmware_device_code_key", F.concat_ws("/", "firmware", "device_code"))
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(5).over(w3).alias("is_new_burst_bucket"),
        F.last("humidity", ignorenulls=True).over(w4).alias("last_known_humidity"),
        F.round((F.col("pressure") / F.sum("pressure").over(w5)), 4).alias("pressure_share"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.percentile_approx("is_new_burst_bucket", 0.9).alias("is_new_burst_bucket_p90"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("is_new_burst_per_dv_02"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.max(F.when(F.col("device_code") == "DV-01", F.col("last_known_humidity"))).alias("dv_01_max_last_known_humidity"),
    )
)
