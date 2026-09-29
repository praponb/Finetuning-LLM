from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("pressure", "id")
w5 = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy(F.col("running_count_pressure").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy("running_count_pressure_bucket", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("prev_pressure", "id")

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

with_windows = (
    sessionized
    .select(
        "*",
        F.count("pressure").over(w3).alias("running_count_pressure"),
        F.min("pressure").over(w3).alias("running_min_pressure"),
        F.max("pressure").over(w3).alias("running_max_pressure"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "running_count_pressure",
        F.lag("pressure", 2).over(w_running).alias("prev_pressure"),
        F.max("session_no").over(w5).alias("device_code_max_session_no"),
        F.ntile(4).over(w6).alias("running_count_pressure_bucket"),
    )
)

metrics = (
    enriched
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "running_count_pressure",
        "prev_pressure",
        "running_count_pressure_bucket",
        F.avg("status_bits").over(w7).alias("status_bits_avg_5"),
        F.sum("temp_c").over(w8).alias("running_temp_c"),
    )
)

with_windows_device_readings = (
    metrics
    .select(
        "id",
        "temp_c",
        "status_bits",
        "firmware",
        "prev_pressure",
        "status_bits_avg_5",
        "running_temp_c",
        F.lag("running_count_pressure").over(w_grp).alias("prev_running_count_pressure"),
        F.lead("running_count_pressure").over(w_grp).alias("next_running_count_pressure"),
        F.lead("status_bits").over(w_grp).alias("next_status_bits"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    with_windows_device_readings
    .groupBy(
        F.when(F.col("running_temp_c") >= 182, "LARGE").otherwise("SMALL").alias("running_temp_c_tier"),
        F.when(F.col("status_bits") >= 10, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
    )
)
