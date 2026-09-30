from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("humidity", "id")
w_grp = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("reading_ts_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("humidity_rounded").desc(), "id")
w6 = Window.orderBy("next_humidity_rounded", "id")
w7 = Window.orderBy("pressure", "id")
w8 = Window.orderBy("first_known_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last_known_first_known_temp_c")
w10 = Window.orderBy(F.col("last_known_first_known_temp_c_dense_rank").asc_nulls_first())
w11 = Window.orderBy("pressure", "id").rowsBetween(-5, -1)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 04:00:00").cast("timestamp")) & F.col("temp_c").between(30, 31))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.initcap(F.trim("device_code")).alias("device_code_title"),
    )
)

cleaned_device_readings = (
    cleaned
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & F.hour("reading_ts").between(6, 11))
    .withColumn("reading_ts_day", F.col("reading_ts").cast("date"))
    .withColumn("humidity_rounded", F.round("humidity", 1))
)

enriched = (
    cleaned_device_readings
    .select(
        "*",
        F.lead("humidity_rounded").over(w_hist).alias("next_humidity_rounded"),
        F.lead("temp_c").over(w_hist).alias("next_temp_c"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.first("temp_c", ignorenulls=True).over(w_grp).alias("first_known_temp_c"),
    )
)

valid = (
    enriched
    .filter(F.col("next_humidity_rounded").between(45, 64))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "device_code_title",
        "reading_ts_day",
        "humidity_rounded",
        "next_humidity_rounded",
        "next_temp_c",
        "first_known_temp_c",
        F.col("first_known_temp_c").cast("int").alias("first_known_temp_c_int"),
    )
)

with_windows = (
    valid
    .withColumn("last_known_first_known_temp_c", F.last("first_known_temp_c", ignorenulls=True).over(w_part))
    .withColumn("running_next_humidity_rounded", F.sum("next_humidity_rounded").over(w4))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("prev_humidity_rounded", F.lag("humidity_rounded", 1).over(w6))
)

enriched_device_readings = (
    with_windows
    .filter(F.col("first_known_temp_c") >= 22)
    .select(
        "id",
        "reading_ts",
        "pressure",
        "device_code_title",
        "last_known_first_known_temp_c",
        "event_seq",
        F.lag("next_temp_c", 2).over(w7).alias("prev_next_temp_c"),
        F.first("next_temp_c", ignorenulls=True).over(w8).alias("first_known_next_temp_c"),
        F.dense_rank().over(w9).alias("last_known_first_known_temp_c_dense_rank"),
    )
)

scored = (
    enriched_device_readings
    .select(
        "id",
        "reading_ts",
        "last_known_first_known_temp_c",
        "prev_next_temp_c",
        "last_known_first_known_temp_c_dense_rank",
        F.cume_dist().over(w10).alias("last_known_first_known_temp_c_dense_rank_cume_dist"),
        F.avg("event_seq").over(w11).alias("event_seq_avg_5_prior"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("day", "reading_ts").alias("activity_day"),
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
        F.when(F.col("prev_next_temp_c") >= 41.3, "PREMIUM").otherwise("STANDARD").alias("prev_next_temp_c_tier"),
    )
    .agg(
        F.sum("event_seq_avg_5_prior").alias("total_event_seq_avg_5_prior"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
