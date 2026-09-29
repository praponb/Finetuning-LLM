from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("humidity").desc(), "id")
w_grp = Window.orderBy("humidity")
w5 = Window.orderBy("pressure", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy("pressure_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("humidity_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("pressure", "id").rowsBetween(-2, -1)
w9 = Window.orderBy("running_min_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("running_max_running_min_temp_c", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.orderBy(F.col("running_avg_running_max_temp_c").desc_nulls_last())
w12 = Window.orderBy("running_sum_running_min_temp_c")
w13 = Window.orderBy("running_count_temp_c", "id")
w14 = Window.orderBy("humidity_bucket", "id")

deduped = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "reading_ts", "temp_c", "humidity", "pressure")
)

with_windows = (
    deduped
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        F.sum("pressure").over(w2).alias("cumulative_pressure"),
        F.ntile(5).over(w3).alias("humidity_bucket"),
        F.rank().over(w_grp).alias("humidity_rank"),
        F.avg("pressure").over(w5).alias("pressure_avg_2"),
    )
)

enriched = (
    with_windows
    .filter(F.col("humidity_bucket").isNull())
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "cumulative_pressure",
        "humidity_bucket",
        F.avg("temp_c").over(w_running).alias("running_avg_temp_c"),
        F.max("temp_c").over(w_running).alias("running_max_temp_c"),
        F.count("temp_c").over(w_running).alias("running_count_temp_c"),
        F.min("temp_c").over(w_running).alias("running_min_temp_c"),
        F.ntile(10).over(w3).alias("humidity_bucket_2"),
    )
)

enriched_device_readings = (
    enriched
    .select(
        "id",
        "pressure",
        "cumulative_pressure",
        "humidity_bucket",
        "running_avg_temp_c",
        "running_max_temp_c",
        "running_count_temp_c",
        "running_min_temp_c",
        F.avg("running_min_temp_c").over(w7).alias("running_avg_running_min_temp_c"),
        F.max("running_min_temp_c").over(w7).alias("running_max_running_min_temp_c"),
        F.count("running_min_temp_c").over(w7).alias("running_count_running_min_temp_c"),
        F.sum("running_min_temp_c").over(w7).alias("running_sum_running_min_temp_c"),
        F.avg("running_avg_temp_c").over(w8).alias("running_avg_temp_c_avg_2_prior"),
    )
)

with_windows_device_readings = (
    enriched_device_readings
    .withColumn("running_avg_running_max_temp_c", F.avg("running_max_temp_c").over(w9))
    .withColumn("running_count_running_max_temp_c", F.count("running_max_temp_c").over(w9))
    .withColumn("running_sum_running_max_temp_c", F.sum("running_max_temp_c").over(w9))
    .withColumn("running_max_temp_c_avg_3", F.avg("running_max_temp_c").over(w10))
)

metrics = (
    with_windows_device_readings
    .select(
        "id",
        "humidity_bucket",
        "running_avg_temp_c",
        "running_max_temp_c",
        "running_min_temp_c",
        "running_max_running_min_temp_c",
        "running_count_running_min_temp_c",
        "running_avg_running_max_temp_c",
        F.cume_dist().over(w11).alias("running_avg_running_max_temp_c_cume_dist"),
        F.cume_dist().over(w12).alias("running_sum_running_min_temp_c_cume_dist"),
        F.lead("running_avg_temp_c_avg_2_prior", 1, 0).over(w13).alias("next_running_avg_temp_c_avg_2_prior"),
        F.sum("running_avg_running_max_temp_c").over(w9).alias("running_running_avg_running_max_temp_c"),
    )
)

result = (
    metrics
    .select(
        "id",
        "humidity_bucket",
        "running_avg_temp_c",
        "running_max_temp_c",
        "running_min_temp_c",
        "running_max_running_min_temp_c",
        "running_count_running_min_temp_c",
        "running_avg_running_max_temp_c",
        "running_sum_running_min_temp_c_cume_dist",
        "next_running_avg_temp_c_avg_2_prior",
        F.lag("humidity_bucket").over(w14).alias("prev_humidity_bucket"),
        F.lag("running_max_running_min_temp_c").over(w14).alias("prev_running_max_running_min_temp_c"),
        F.lead("running_max_running_min_temp_c").over(w14).alias("next_running_max_running_min_temp_c"),
        F.row_number().over(w14).alias("seq_no"),
    )
)
