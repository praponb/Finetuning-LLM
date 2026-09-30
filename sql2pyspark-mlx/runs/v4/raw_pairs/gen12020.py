from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.floor("status_bits").alias("status_bits_whole"),
        F.col("pressure").cast("int").alias("pressure_int"),
    )
)

with_prev = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        "device_code",
        F.when(F.col("pressure_int") >= 80, "LARGE").otherwise("SMALL").alias("pressure_int_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("session_start_flag_per_dv_02"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
    )
    .filter(F.col("dv_01_rows") > 1)
    .orderBy(F.col("span_minutes").asc_nulls_first())
)
