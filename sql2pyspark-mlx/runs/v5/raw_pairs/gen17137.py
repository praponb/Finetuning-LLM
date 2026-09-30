from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("temp_c_int", "id")
w_part = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.orderBy("pressure", "id")
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy("temp_c_int_count_1h", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("temp_c_int_count_1h_avg_3", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        F.concat_ws("/", "device_code", "firmware").alias("device_code_firmware_key"),
        F.col("temp_c").cast("int").alias("temp_c_int"),
    )
)

scored = (
    valid
    .filter(F.col("status_bits") < 8)
    .select(
        "id",
        "reading_ts",
        "pressure",
        "temp_c_int",
        F.lag("status_bits").over(w_ordered).alias("prev_status_bits"),
        F.lag("temp_c").over(w_ordered).alias("prev_temp_c"),
        F.lead("temp_c").over(w_ordered).alias("next_temp_c"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.count("temp_c_int").over(w_part).alias("temp_c_int_count_1h"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "temp_c_int",
        "next_temp_c",
        F.ntile(4).over(w_running).alias("pressure_bucket"),
        F.sum("pressure").over(w4).alias("pressure_sum_2h"),
        F.avg("temp_c_int_count_1h").over(w5).alias("temp_c_int_count_1h_avg_3"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "temp_c_int_count_1h_avg_3",
        F.avg("pressure_sum_2h").over(w6).alias("last7_avg_pressure_sum_2h"),
        F.count("pressure_sum_2h").over(w6).alias("last7_count_pressure_sum_2h"),
        F.sum("pressure_sum_2h").over(w6).alias("last7_sum_pressure_sum_2h"),
    )
)

filtered = (
    metrics
    .filter(F.col("last7_sum_pressure_sum_2h") < 16000)
    .select(
        "id",
        "temp_c_int_count_1h_avg_3",
        "last7_avg_pressure_sum_2h",
        "last7_count_pressure_sum_2h",
        (F.col("last7_count_pressure_sum_2h") * F.col("last7_sum_pressure_sum_2h")).alias("last7_count_pressure_sum_2h_times_last7_sum_pressure_sum_2h"),
    )
)

result = (
    filtered
    .withColumn("last7_count_pressure_sum_2h_tier", F.when(F.col("last7_count_pressure_sum_2h") >= 13, "PREMIUM").otherwise("STANDARD"))
    .withColumn("temp_c_int_count_1h_avg_3_tier", F.when(F.col("temp_c_int_count_1h_avg_3") >= 15, "HIGH").otherwise("LOW"))
    .withColumn("last7_avg_pressure_sum_2h_tier", F.when(F.col("last7_avg_pressure_sum_2h") >= 4200, "HIGH").otherwise("LOW"))
    .rollup(
        "last7_count_pressure_sum_2h_tier",
        "temp_c_int_count_1h_avg_3_tier",
        "last7_avg_pressure_sum_2h_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("temp_c_int_count_1h_avg_3", [0.25, 0.5, 0.75]).alias("temp_c_int_count_1h_avg_3_quartiles"),
        F.grouping("last7_count_pressure_sum_2h_tier").alias("grouping_id"),
    )
    .filter(F.col("cnt") < 1000)
    .orderBy(F.col("temp_c_int_count_1h_avg_3_tier").desc())
)
