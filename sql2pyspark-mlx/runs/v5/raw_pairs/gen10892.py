from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"), "device_code")
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.min("reading_ts").alias("first_reading_ts"),
    )
    .orderBy(F.col("distinct_firmwares").asc_nulls_last())
)
