from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("running_max_status_bits", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w5 = Window.partitionBy("device_code")
w6 = Window.orderBy("running_count_status_bits", "id")

scored = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.count("status_bits").over(w_running).alias("running_count_status_bits"),
        F.min("status_bits").over(w_running).alias("running_min_status_bits"),
        F.max("status_bits").over(w_running).alias("running_max_status_bits"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "running_count_status_bits",
        F.avg("temp_c").over(w2).alias("temp_c_avg_3"),
        F.row_number().over(w3).alias("event_seq"),
        F.percent_rank().over(w4).alias("humidity_percent_rank"),
        F.round((F.col("running_count_status_bits") / F.sum("running_count_status_bits").over(w5)), 4).alias("running_count_status_bits_share"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

remaining_keys = (
    metrics
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "1.1.0")
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("status_bits") == 10)
    .select(
        "id",
        "humidity",
        "status_bits",
        "running_count_status_bits",
        "temp_c_avg_3",
        "humidity_percent_rank",
        "row_seq",
        F.round((F.col("row_seq") * 1.07), 2).alias("row_seq_adjusted"),
    )
)

result = (
    base
    .select("running_count_status_bits", "humidity_percent_rank", "row_seq_adjusted")
)
