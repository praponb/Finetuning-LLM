from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("pressure").between(1022, 1026))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        (F.col("pressure") + F.col("status_bits")).alias("pressure_plus_status_bits"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .groupingSets(
        [["hour_window", "session_no"], ["hour_window"], []],
        "hour_window",
        "session_no",
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("temp_c")).otherwise(0)).alias("dv_04_temp_c"),
        F.min("reading_ts").alias("started_at"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("duration_min").desc_nulls_last())
)
