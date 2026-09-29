from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.orderBy(F.col("running_min_session_no").desc(), "id")
w_running = Window.partitionBy("device_code").orderBy("running_avg_session_no", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, -1)

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.when(F.col("device_code").isin(["DV-02", "DV-01"]), "Core").otherwise("Other").alias("device_code_group"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

metrics = (
    sessionized
    .filter(F.col("humidity").between(55, 64))
    .select(
        "id",
        "device_code",
        "humidity",
        "session_no",
        F.sum("session_no").over(w_hist).alias("running_sum_session_no"),
        F.min("session_no").over(w_hist).alias("running_min_session_no"),
        F.avg("session_no").over(w_hist).alias("running_avg_session_no"),
        F.row_number().over(w_hist_2).alias("row_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("running_sum_session_no") < 36)
    .select(
        "id",
        "device_code",
        "humidity",
        "session_no",
        "running_sum_session_no",
        "running_min_session_no",
        "running_avg_session_no",
        "row_seq",
        (F.col("session_no") - F.col("running_sum_session_no")).alias("session_no_minus_running_sum_session_no"),
    )
)

filtered_device_readings = (
    filtered
    .filter(F.col("humidity") <= 73)
    .select(
        "*",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        F.coalesce("session_no_minus_running_sum_session_no", F.lit(0)).alias("session_no_minus_running_sum_session_no_filled"),
    )
)

scored = (
    filtered_device_readings
    .withColumn("row_seq_2", F.row_number().over(w5))
    .withColumn("running_sum_session_no_avg_3", F.avg("running_sum_session_no").over(w_running))
    .withColumn("row_seq_avg_3_prior", F.avg("row_seq").over(w7))
)

result = (
    scored
    .groupBy(
        "device_code",
        F.when(F.col("running_avg_session_no") >= 8.0, "HIGH").otherwise("LOW").alias("running_avg_session_no_tier"),
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("humidity_filled"))).alias("dv_04_avg_humidity_filled"),
        F.percentile_approx("running_avg_session_no", [0.25, 0.5, 0.75]).alias("running_avg_session_no_quartiles"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("running_avg_session_no_tier").asc_nulls_last())
)
