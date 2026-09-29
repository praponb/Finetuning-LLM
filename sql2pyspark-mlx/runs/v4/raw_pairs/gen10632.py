from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits_avg_3_prior").asc_nulls_last())
w_ordered = Window.partitionBy("device_code").orderBy(F.col("running_temp_c").desc(), "id")
w_grp = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first())
w8 = Window.orderBy("status_bits_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w10 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w12 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("device_code").orderBy(F.col("status_bits_avg_3_prior").desc_nulls_first(), "id")

metrics = (
    df_device_readings
    .select(
        "*",
        F.sum("temp_c").over(w).alias("running_temp_c"),
        F.avg("status_bits").over(w2).alias("status_bits_avg_3_prior"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("running_temp_c_bucket", F.ntile(4).over(w_ordered))
    .withColumn("last_known_running_temp_c", F.last("running_temp_c", ignorenulls=True).over(w_grp))
    .withColumn("temp_c_avg_5", F.avg("temp_c").over(w6))
    .withColumn("pressure_cume_dist", F.cume_dist().over(w_part))
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w8))
)

with_prev = scored.withColumn("prev_reading_ts", F.lag("reading_ts").over(w9))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w10))

base = (
    islands
    .filter(F.col("status_bits_avg_3_prior") >= 10)
    .withColumn("last_known_running_temp_c_whole", F.ceil("last_known_running_temp_c"))
    .withColumn("temp_c_avg_5_minus_pressure", (F.col("temp_c_avg_5") - F.col("pressure")))
)

gaps_2 = (
    base
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts") - F.lag("prev_reading_ts").over(w11) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_device_readings = gaps_2.withColumn("burst_id", F.sum("session_start_flag").over(w12))

deduped = (
    islands_device_readings
    .withColumn("latest_rank", F.row_number().over(w13))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
        F.when(F.col("burst_id") >= 6, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("prev_reading_ts")) - F.unix_timestamp(F.min("prev_reading_ts"))) / 60).alias("duration_min"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.min("prev_reading_ts").alias("started_at"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
    )
    .filter(F.col("duration_min") < 0)
)
