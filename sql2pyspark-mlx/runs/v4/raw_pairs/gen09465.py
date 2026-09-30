from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .withColumn("last_known_temp_c", F.last("temp_c", ignorenulls=True).over(w_recent))
    .withColumn("cumulative_humidity", F.sum("humidity").over(w4))
    .withColumn("humidity_rank", F.rank().over(w5))
)

filtered = (
    with_windows
    .filter(F.col("session_start_flag") == 1)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "session_start_flag",
        "burst_id",
        "last_known_temp_c",
        "cumulative_humidity",
        F.concat("firmware", F.lit(" - "), "device_code").alias("firmware_device_code"),
    )
)

totals = (
    filtered
    .rollup(
        F.when(F.col("burst_id") >= 9, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
    )
    .filter(F.col("num_device_codes") > 1)
)

result = totals.select("burst_id_tier", "num_device_codes", "dv_02_events", "dv_03_events")
