from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("status_bits", "id")
w_recent = Window.orderBy(F.col("temp_c").desc())
w4 = Window.orderBy(F.col("status_bits_bucket").desc(), "id")
w5 = Window.orderBy("running_count_temp_c", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp")) & (F.col("reading_ts") >= "2025-03-01"))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("temp_c").cast("int").alias("temp_c_int"),
        F.lower("device_code").alias("device_code_lower"),
    )
)

with_windows = (
    valid
    .filter(F.col("status_bits") == 12)
    .select(
        "id",
        "temp_c",
        "pressure",
        F.count("temp_c").over(w_running).alias("running_count_temp_c"),
        F.min("temp_c").over(w_running).alias("running_min_temp_c"),
        F.sum("temp_c").over(w_running).alias("running_sum_temp_c"),
        F.avg("temp_c").over(w_running).alias("running_avg_temp_c"),
        F.ntile(4).over(w_grp).alias("status_bits_bucket"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "running_min_temp_c",
        "running_sum_temp_c",
        "running_avg_temp_c",
        F.dense_rank().over(w_recent).alias("temp_c_dense_rank"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("temp_c").over(w5).alias("temp_c_avg_6"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

base = (
    enriched
    .filter(F.col("row_seq") >= 3)
    .select(
        "id",
        "running_avg_temp_c",
        "temp_c_avg_6",
        "row_seq",
        F.when(F.col("running_sum_temp_c") >= 147, "High").when(F.col("running_sum_temp_c") >= 49, "Medium").otherwise("Low").alias("running_sum_temp_c_band"),
    )
)

result = (
    base
    .select("id", "running_avg_temp_c", "temp_c_avg_6", "running_sum_temp_c_band")
    .orderBy(F.col("id").desc_nulls_last())
)
