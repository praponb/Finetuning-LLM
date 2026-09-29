from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts_date").desc(), "id")
w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("reading_ts", "id").rowsBetween(-6, -1)
w6 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("temp_c_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.orderBy("cumulative_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("row_seq").desc(), "id")
w11 = Window.orderBy("running_max_running_last_known_pressure", "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .select("*", F.to_date("reading_ts").alias("reading_ts_date"))
)

scored = (
    valid
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.row_number().over(w).alias("row_seq"),
        F.avg("pressure").over(w_ordered).alias("pressure_avg_3"),
        F.last("pressure", ignorenulls=True).over(w_grp).alias("last_known_pressure"),
    )
)

enriched = (
    scored
    .filter(F.col("reading_ts") < "2024-03-01")
    .select(
        "id",
        "firmware",
        "row_seq",
        "pressure_avg_3",
        F.sum("last_known_pressure").over(w4).alias("running_last_known_pressure"),
        F.avg("temp_c").over(w5).alias("temp_c_avg_6_prior"),
        F.sum("temp_c").over(w6).alias("cumulative_temp_c"),
    )
)

enriched_device_readings = (
    enriched
    .withColumn("running_sum_running_last_known_pressure", F.sum("running_last_known_pressure").over(w7))
    .withColumn("running_max_running_last_known_pressure", F.max("running_last_known_pressure").over(w7))
    .withColumn("running_count_running_last_known_pressure", F.count("running_last_known_pressure").over(w7))
    .withColumn("running_avg_running_last_known_pressure", F.avg("running_last_known_pressure").over(w7))
    .withColumn("running_last_known_pressure_avg_2", F.avg("running_last_known_pressure").over(w8))
)

scored_device_readings = (
    enriched_device_readings
    .withColumn("running_avg_running_sum_running_last_known_pressure", F.avg("running_sum_running_last_known_pressure").over(w_recent))
    .withColumn("running_count_running_sum_running_last_known_pressure", F.count("running_sum_running_last_known_pressure").over(w_recent))
    .withColumn("running_min_running_sum_running_last_known_pressure", F.min("running_sum_running_last_known_pressure").over(w_recent))
)

metrics = (
    scored_device_readings
    .select(
        "id",
        "pressure_avg_3",
        "temp_c_avg_6_prior",
        "cumulative_temp_c",
        "running_sum_running_last_known_pressure",
        "running_max_running_last_known_pressure",
        "running_count_running_last_known_pressure",
        "running_avg_running_last_known_pressure",
        "running_avg_running_sum_running_last_known_pressure",
        "running_min_running_sum_running_last_known_pressure",
        F.ntile(4).over(w10).alias("row_seq_bucket"),
        F.lead("running_max_running_last_known_pressure", 1, 0).over(w11).alias("next_running_max_running_last_known_pressure"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("pressure_avg_3") >= 1032, "PREMIUM").otherwise("STANDARD").alias("pressure_avg_3_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_running_max_running_last_known_pressure", [0.25, 0.5, 0.75]).alias("next_running_max_running_last_known_pressure_quartiles"),
        F.percentile_approx("row_seq_bucket", [0.25, 0.5, 0.75]).alias("row_seq_bucket_quartiles"),
    )
)
