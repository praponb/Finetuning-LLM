from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.orderBy("reading_ts", "id")
w3 = Window.orderBy(F.col("reading_ts_day").desc(), "id")
w_recent = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 06:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.round("humidity", 0).alias("humidity_rounded"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "reading_ts", "firmware")
)

cleaned = (
    ranked
    .filter(F.col("reading_ts") >= "2023-08-01")
    .select("*", F.col("reading_ts").cast("date").alias("reading_ts_day"))
)

with_windows = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("row_seq_2", F.row_number().over(w3))
)

with_windows_device_readings = (
    with_windows
    .withColumn("running_max_row_seq_2", F.max("row_seq_2").over(w_recent))
    .withColumn("running_avg_row_seq_2", F.avg("row_seq_2").over(w_recent))
    .withColumn("running_min_row_seq_2", F.min("row_seq_2").over(w_recent))
)

cleaned_device_readings = (
    with_windows_device_readings
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 04:00:00"))) & (F.year("reading_ts_day") == 2025))
    .select(
        "id",
        "firmware",
        "reading_ts_day",
        "seq_no",
        "row_seq",
        "row_seq_2",
        "running_min_row_seq_2",
        (F.col("running_max_row_seq_2") * F.col("seq_no")).alias("running_max_row_seq_2_times_seq_no"),
    )
)

result = (
    cleaned_device_readings
    .select("id", "firmware", "reading_ts_day", "row_seq_2", "running_min_row_seq_2")
    .orderBy("reading_ts_day", F.col("running_min_row_seq_2").desc())
)
