from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("next_temp_c_times_humidity", "id")
w4 = Window.partitionBy("device_code").orderBy("temp_c_times_humidity", "id")
w5 = Window.partitionBy("device_code").orderBy("seq_no", "id")
w6 = Window.orderBy("status_bits_rounded", "id")
w7 = Window.orderBy(F.col("prev_status_bits").asc_nulls_last())

base = (
    df_device_readings
    .filter(F.col("humidity") < 71)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        (F.col("temp_c") * F.col("humidity")).alias("temp_c_times_humidity"),
        (F.col("temp_c") * F.col("humidity")).alias("temp_c_times_humidity_2"),
    )
)

scored = (
    base
    .withColumn("next_temp_c_times_humidity", F.lead("temp_c_times_humidity").over(w))
    .withColumn("prev_temp_c", F.lag("temp_c").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

with_windows = (
    scored
    .select(
        "id",
        "device_code",
        "status_bits",
        "temp_c_times_humidity_2",
        "prev_temp_c",
        "seq_no",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.lag("next_temp_c_times_humidity", 1).over(w4).alias("prev_next_temp_c_times_humidity"),
        F.lag("status_bits", 1).over(w5).alias("prev_status_bits"),
    )
)

filtered = (
    with_windows
    .filter(F.col("device_code") == "DV-03")
    .select(
        "id",
        "status_bits",
        "temp_c_times_humidity_2",
        "prev_temp_c",
        "event_seq_2",
        "prev_next_temp_c_times_humidity",
        "prev_status_bits",
        F.col("prev_status_bits").cast("string").alias("prev_status_bits_num"),
    )
)

cleaned = (
    filtered
    .filter(F.col("event_seq_2") <= 8)
    .select("*", F.round("status_bits", 2).alias("status_bits_rounded"))
)

cleaned_device_readings = (
    cleaned
    .filter(F.col("status_bits") >= 9)
    .select(
        "*",
        (F.col("prev_status_bits") * F.col("prev_next_temp_c_times_humidity")).alias("prev_status_bits_times_prev_next_temp_c_times_humidity"),
        F.round((F.col("status_bits_rounded") * 1.1), 2).alias("adjusted_status_bits_rounded"),
    )
)

metrics = (
    cleaned_device_readings
    .filter(F.col("status_bits") < 4)
    .withColumn("prev_adjusted_status_bits_rounded", F.lag("adjusted_status_bits_rounded").over(w6))
    .withColumn("next_adjusted_status_bits_rounded", F.lead("adjusted_status_bits_rounded").over(w6))
    .withColumn("next_status_bits", F.lead("status_bits").over(w6))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("prev_status_bits_cume_dist", F.cume_dist().over(w7))
)

summary = (
    metrics
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL"))
    .withColumn("prev_temp_c_tier", F.when(F.col("prev_temp_c") >= 34.1, "PREMIUM").otherwise("STANDARD"))
    .withColumn("prev_next_temp_c_times_humidity_tier", F.when(F.col("prev_next_temp_c_times_humidity") >= 68, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["row_seq_tier", "prev_temp_c_tier", "prev_next_temp_c_times_humidity_tier"], ["row_seq_tier", "prev_temp_c_tier"], ["row_seq_tier"]],
        "row_seq_tier",
        "prev_temp_c_tier",
        "prev_next_temp_c_times_humidity_tier",
    )
    .agg(
        F.percentile_approx("prev_status_bits", [0.25, 0.5, 0.75]).alias("prev_status_bits_quartiles"),
        F.round(F.avg("adjusted_status_bits_rounded"), 2).alias("avg_adjusted_status_bits_rounded"),
        F.percentile_approx("adjusted_status_bits_rounded", [0.25, 0.5, 0.75]).alias("adjusted_status_bits_rounded_quartiles"),
        F.grouping("row_seq_tier").alias("gid"),
    )
)

result = (
    summary
    .select(
        "row_seq_tier",
        "prev_temp_c_tier",
        "prev_next_temp_c_times_humidity_tier",
        "prev_status_bits_quartiles",
        "avg_adjusted_status_bits_rounded",
        "gid",
    )
    .orderBy(F.col("row_seq_tier").asc_nulls_first())
)
