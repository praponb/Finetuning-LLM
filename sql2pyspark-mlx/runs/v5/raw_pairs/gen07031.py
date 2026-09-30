from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("humidity", "id")
w_grp = Window.orderBy(F.col("temp_c").desc(), "id")
w4 = Window.orderBy(F.col("humidity").desc(), "id")
w5 = Window.orderBy(F.col("last_known_humidity").desc(), "id")
w6 = Window.orderBy(F.col("event_seq").desc(), "id")
w7 = Window.orderBy("event_seq_2", "id")
w8 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("status_bits") == 9)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.substring("firmware", 1, 4).alias("firmware_prefix"),
    )
)

cleaned = (
    base
    .filter(F.col("status_bits") >= 4)
    .select("*", F.substring("firmware", 1, 2).alias("firmware_prefix_2"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "firmware_prefix",
        "firmware_prefix_2",
        F.last("humidity", ignorenulls=True).over(w).alias("last_known_humidity"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        "firmware_prefix",
        "last_known_humidity",
        "event_seq",
        F.ntile(5).over(w4).alias("humidity_bucket"),
        F.row_number().over(w5).alias("event_seq_2"),
        F.ntile(4).over(w6).alias("event_seq_bucket"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "firmware",
        "firmware_prefix",
        "last_known_humidity",
        "event_seq_2",
        "event_seq_bucket",
        F.lead("event_seq_bucket", 1).over(w7).alias("next_event_seq_bucket"),
        F.sum("event_seq").over(w8).alias("running_event_seq"),
    )
)

totals = (
    metrics_device_readings
    .groupBy(
        F.when(F.col("event_seq_2") >= 7, "HIGH").otherwise("LOW").alias("event_seq_2_tier"),
        F.when(F.col("last_known_humidity") >= 83, "HIGH").otherwise("LOW").alias("last_known_humidity_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.round(F.avg("event_seq_2"), 2).alias("avg_event_seq_2"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.round(F.avg("event_seq_2"), 2).alias("avg_event_seq_2_2"),
    )
    .filter(F.col("cnt") >= 1)
)

result = (
    totals
    .select(
        "event_seq_2_tier",
        "last_known_humidity_tier",
        "cnt",
        "distinct_firmwares",
        "avg_event_seq_2_2",
    )
    .orderBy("distinct_firmwares")
)
