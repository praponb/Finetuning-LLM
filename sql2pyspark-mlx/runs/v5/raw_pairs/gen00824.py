from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

with_windows = (
    df_device_readings
    .select(
        "*",
        F.ntile(5).over(w).alias("temp_c_bucket"),
        F.last("temp_c", ignorenulls=True).over(w2).alias("last_known_temp_c"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w3).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w3) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_bucket",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

deduped = (
    sessions_device_readings
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_bucket",
    )
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

daily = (
    latest
    .withColumn("temp_c_bucket_tier", F.when(F.col("temp_c_bucket") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 65, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["temp_c_bucket_tier", "humidity_tier"], ["temp_c_bucket_tier"], []],
        "temp_c_bucket_tier",
        "humidity_tier",
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("temp_c_bucket"))).alias("dv_01_total_temp_c_bucket"),
        F.min("humidity").alias("min_humidity"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
        F.corr("humidity", "status_bits").alias("corr_humidity_status_bits"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.corr("pressure", "humidity").alias("corr_pressure_humidity"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    daily
    .select(
        "temp_c_bucket_tier",
        "humidity_tier",
        "dv_01_total_temp_c_bucket",
        "min_humidity",
        "corr_humidity_status_bits",
        "dv_03_events",
        "corr_pressure_humidity",
        "gid",
    )
    .orderBy(F.col("dv_01_total_temp_c_bucket").asc_nulls_last())
)
