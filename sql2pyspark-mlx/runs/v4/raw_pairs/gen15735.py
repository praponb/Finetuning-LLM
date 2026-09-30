from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w2 = Window.partitionBy("device_code")
w3 = Window.orderBy("pressure", "id")
w_recent = Window.orderBy("corr_pressure_status_bits_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("corr_pressure_status_bits_scaled", "id")
w6 = Window.orderBy(F.col("pressure").asc_nulls_first())
w7 = Window.orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy("status_bits", "id")
w9 = Window.orderBy("seq_no_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 15:00:00").cast("timestamp")) & (F.to_date("reading_ts") >= "2025-09-21"))
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") * F.col("temp_c")).alias("humidity_times_temp_c"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.corr("pressure", "status_bits").over(w2).alias("corr_pressure_status_bits"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

filtered = (
    enriched
    .filter(F.col("pressure") >= 1022)
    .select(
        "id",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_times_temp_c",
        "corr_pressure_status_bits",
        F.round((F.col("corr_pressure_status_bits") * 1.1), 2).alias("corr_pressure_status_bits_scaled"),
        F.floor("seq_no").alias("seq_no_whole"),
    )
)

enriched_device_readings = (
    filtered
    .select(
        "*",
        F.sum("corr_pressure_status_bits").over(w_recent).alias("running_corr_pressure_status_bits"),
        F.lead("corr_pressure_status_bits_scaled", 2).over(w5).alias("next_corr_pressure_status_bits_scaled"),
        F.dense_rank().over(w6).alias("pressure_dense_rank"),
        F.sum("corr_pressure_status_bits_scaled").over(w_recent).alias("cumulative_corr_pressure_status_bits_scaled"),
    )
)

scored = (
    enriched_device_readings
    .withColumn("status_bits_avg_6", F.avg("status_bits").over(w7))
    .withColumn("prev_corr_pressure_status_bits", F.lag("corr_pressure_status_bits", 2).over(w8))
    .withColumn("last_known_humidity_times_temp_c", F.last("humidity_times_temp_c", ignorenulls=True).over(w9))
)

result = (
    scored
    .select(
        "id",
        "status_bits",
        "humidity_times_temp_c",
        "corr_pressure_status_bits",
        "corr_pressure_status_bits_scaled",
        "running_corr_pressure_status_bits",
        "next_corr_pressure_status_bits_scaled",
        "pressure_dense_rank",
        "cumulative_corr_pressure_status_bits_scaled",
    )
)
