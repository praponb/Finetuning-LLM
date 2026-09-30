from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w_running = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_part = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy("pressure")
w6 = Window.partitionBy("device_code")
w7 = Window.partitionBy("device_code").orderBy("next_humidity", "id")
w8 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w9 = Window.orderBy(F.col("seq_no").asc_nulls_last())
w10 = Window.partitionBy("device_code").orderBy(F.col("next_humidity_2").desc_nulls_last())
w11 = Window.partitionBy("device_code").orderBy("pressure_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("device_code_avg_humidity", "id")
w13 = Window.orderBy(F.col("temp_c").desc(), "id")
w14 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w15 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "*",
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("pressure").over(w_running).alias("running_pressure"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lead("humidity", 1).over(w3).alias("next_humidity"),
        F.sum("humidity").over(w_part).alias("humidity_sum_1h"),
    )
)

metrics_device_readings = (
    metrics
    .withColumn("pressure_dense_rank", F.dense_rank().over(w5))
    .withColumn("device_code_avg_humidity", F.avg("humidity").over(w6))
    .withColumn("next_humidity_2", F.lead("humidity", 1).over(w7))
    .withColumn("seq_no_count_30m", F.count("seq_no").over(w8))
)

metrics_device_readings_2 = (
    metrics_device_readings
    .select(
        "*",
        F.dense_rank().over(w9).alias("seq_no_dense_rank"),
        F.percent_rank().over(w10).alias("next_humidity_2_percent_rank"),
        F.sum("pressure_dense_rank").over(w11).alias("running_pressure_dense_rank"),
    )
)

with_windows = (
    metrics_device_readings_2
    .withColumn("next_seq_no_count_30m", F.lead("seq_no_count_30m", 2, 0).over(w_grp))
    .withColumn("seq_no_2", F.row_number().over(w13))
    .withColumn("seq_no_sum_2h", F.sum("seq_no").over(w14))
    .withColumn("last_known_pressure_dense_rank", F.last("pressure_dense_rank", ignorenulls=True).over(w15))
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count_2"),
    )
)
