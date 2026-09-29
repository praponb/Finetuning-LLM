from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w_grp = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy("status_bits_max_30m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("running_sum_device_code_sum_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("running_max_device_code_sum_temp_c", "id")
w7 = Window.orderBy("cumulative_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("cumulative_cumulative_pressure", "id")

metrics = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.sum("temp_c").over(w).alias("device_code_sum_temp_c"),
        F.max("status_bits").over(w_grp).alias("status_bits_max_30m"),
        F.sum("pressure").over(w3).alias("cumulative_pressure"),
    )
)

with_windows = (
    metrics
    .filter(F.col("status_bits_max_30m") == 5)
    .select(
        "id",
        "device_code",
        "device_code_sum_temp_c",
        "cumulative_pressure",
        F.max("device_code_sum_temp_c").over(w_hist).alias("running_max_device_code_sum_temp_c"),
        F.min("device_code_sum_temp_c").over(w_hist).alias("running_min_device_code_sum_temp_c"),
        F.sum("device_code_sum_temp_c").over(w_hist).alias("running_sum_device_code_sum_temp_c"),
    )
)

base = (
    with_windows
    .filter(F.col("device_code_sum_temp_c") != 114)
    .withColumn("least_device_code_sum_temp_c_running_sum_device_code_sum_temp_c", F.least("device_code_sum_temp_c", "running_sum_device_code_sum_temp_c"))
)

enriched = (
    base
    .select(
        "*",
        F.avg("device_code_sum_temp_c").over(w).alias("device_code_avg_device_code_sum_temp_c"),
        F.sum("running_min_device_code_sum_temp_c").over(w5).alias("running_running_min_device_code_sum_temp_c"),
        F.lead("device_code_sum_temp_c", 2).over(w6).alias("next_device_code_sum_temp_c"),
        F.sum("cumulative_pressure").over(w7).alias("cumulative_cumulative_pressure"),
    )
)

scored = (
    enriched
    .withColumn("next_running_max_device_code_sum_temp_c", F.lead("running_max_device_code_sum_temp_c").over(w8))
    .withColumn("prev_running_max_device_code_sum_temp_c", F.lag("running_max_device_code_sum_temp_c").over(w8))
    .withColumn("prev_running_min_device_code_sum_temp_c", F.lag("running_min_device_code_sum_temp_c").over(w8))
    .withColumn("next_running_min_device_code_sum_temp_c", F.lead("running_min_device_code_sum_temp_c").over(w8))
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("device_code_max_next_device_code_sum_temp_c", F.max("next_device_code_sum_temp_c").over(w))
)

result = (
    scored
    .groupBy(
        F.when(F.col("next_running_min_device_code_sum_temp_c") >= 149, "LARGE").otherwise("SMALL").alias("next_running_min_device_code_sum_temp_c_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count("*").alias("event_count_2"),
    )
    .orderBy(F.col("num_device_codes").desc())
)
