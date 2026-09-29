from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("pressure_per_running_sum_pressure_times_seq_no", "id")
w_recent = Window.orderBy("running_min_pressure_int", "id")

metrics = (
    df_device_readings
    .select(
        "*",
        F.lag("temp_c").over(w).alias("prev_temp_c"),
        F.lead("temp_c").over(w).alias("next_temp_c"),
        F.lead("humidity").over(w).alias("next_humidity"),
        F.row_number().over(w).alias("seq_no"),
        F.last("pressure", ignorenulls=True).over(w2).alias("last_known_pressure"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "next_temp_c",
        "next_humidity",
        "seq_no",
    )
)

with_windows = (
    latest
    .filter(F.col("pressure") < 1009)
    .select(
        "*",
        F.count("pressure").over(w4).alias("running_count_pressure"),
        F.min("pressure").over(w4).alias("running_min_pressure"),
        F.max("pressure").over(w4).alias("running_max_pressure"),
        F.sum("pressure").over(w4).alias("running_sum_pressure"),
    )
)

valid = (
    with_windows
    .filter(F.col("running_count_pressure") >= 16)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "next_temp_c",
        "seq_no",
        "running_max_pressure",
        "running_sum_pressure",
        F.col("running_min_pressure").cast("int").alias("running_min_pressure_int"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("status_bits").between(8, 10))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "next_temp_c",
        "seq_no",
        "running_max_pressure",
        "running_min_pressure_int",
        F.round((F.col("pressure") / F.col("running_sum_pressure")), 2).alias("pressure_per_running_sum_pressure"),
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

filtered = (
    valid_device_readings
    .filter(F.col("temp_c") <= 31)
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "next_temp_c",
        "seq_no",
        "running_max_pressure",
        "running_min_pressure_int",
        (F.col("pressure_per_running_sum_pressure") * F.col("seq_no")).alias("pressure_per_running_sum_pressure_times_seq_no"),
        F.greatest("next_temp_c", "temp_c").alias("greatest_next_temp_c_temp_c"),
    )
)

enriched = (
    filtered
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("running_min_pressure_int_bucket", F.ntile(3).over(w_recent))
)

result = (
    enriched
    .select(
        "id",
        "temp_c",
        "pressure",
        "next_temp_c",
        "seq_no",
        "running_min_pressure_int",
        "pressure_per_running_sum_pressure_times_seq_no",
        "greatest_next_temp_c_temp_c",
        "event_seq",
    )
    .orderBy("id")
    .limit(20)
)
