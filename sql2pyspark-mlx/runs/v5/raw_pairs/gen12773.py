from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("temp_c_minus_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.orderBy(F.col("running_avg_status_bits").desc(), "id")
w4 = Window.orderBy("running_avg_status_bits_per_running_sum_status_bits", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("temp_c_minus_pressure", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
)

cleaned = (
    base
    .filter(F.col("reading_ts") < "2023-06-17")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "temp_c_int",
        (F.col("temp_c") - F.col("pressure")).alias("temp_c_minus_pressure"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "id",
        "temp_c_minus_pressure",
        F.sum("status_bits").over(w_running).alias("running_sum_status_bits"),
        F.avg("status_bits").over(w_running).alias("running_avg_status_bits"),
        F.min("status_bits").over(w_running).alias("running_min_status_bits"),
        F.row_number().over(w_running_2).alias("seq_no"),
    )
)

filtered = (
    with_windows
    .filter(F.col("temp_c_minus_pressure") >= 76)
    .select(
        "*",
        (F.col("running_avg_status_bits") / F.nullif("running_sum_status_bits", F.lit(0))).alias("running_avg_status_bits_per_running_sum_status_bits"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.avg("running_sum_status_bits").over(w4).alias("running_sum_status_bits_avg_2"),
        F.avg("seq_no").over(w5).alias("seq_no_avg_2"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("running_sum_status_bits_avg_2", [0.25, 0.5, 0.75]).alias("running_sum_status_bits_avg_2_quartiles"),
        F.round(F.avg("running_sum_status_bits"), 2).alias("avg_running_sum_status_bits"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("avg_running_sum_status_bits")
)
