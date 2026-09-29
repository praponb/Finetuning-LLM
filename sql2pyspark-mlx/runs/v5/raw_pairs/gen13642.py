from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 03:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.lower("firmware").alias("firmware_lower"),
        F.round((F.col("pressure") * 0.85), 2).alias("pressure_adjusted"),
    )
)

prev_events = valid.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("temp_c") >= 37.9, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("prev_reading_ts")) - F.unix_timestamp(F.min("prev_reading_ts"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("burst_id"))).alias("dv_02_total_burst_id"),
        F.corr("temp_c", "pressure").alias("corr_temp_c_pressure"),
    )
    .filter(F.col("dv_02_total_burst_id") > 0)
    .orderBy(F.col("temp_c_tier").desc_nulls_last())
)
