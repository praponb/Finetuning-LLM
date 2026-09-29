from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w4 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("status_bits_rank", "id")
w6 = Window.orderBy("corr_humidity_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("corr_humidity_pressure").desc_nulls_first())
w8 = Window.orderBy("pressure", "id")
w9 = Window.orderBy("running_temp_c_avg_3_prior", "id")
w10 = Window.orderBy("running_status_bits_rank_bucket", "id").rowsBetween(-6, Window.currentRow)
w11 = Window.orderBy("prev_status_bits_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .filter(F.col("status_bits").between(7, 10))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.corr("humidity", "pressure").over(w).alias("corr_humidity_pressure"),
        F.avg("temp_c").over(w2).alias("temp_c_avg_3_prior"),
        F.rank().over(w_ordered).alias("status_bits_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("temp_c") > 26)
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1026, "High").when(F.col("pressure") >= 1000, "Medium").otherwise("Low"))
    .withColumn("corr_humidity_pressure_whole", F.ceil("corr_humidity_pressure"))
)

enriched_device_readings = (
    valid
    .select(
        "id",
        "humidity",
        "pressure",
        "corr_humidity_pressure",
        "status_bits_rank",
        "pressure_tier",
        "corr_humidity_pressure_whole",
        F.sum("temp_c_avg_3_prior").over(w4).alias("running_temp_c_avg_3_prior"),
        F.ntile(3).over(w5).alias("status_bits_rank_bucket"),
        F.sum("pressure").over(w6).alias("running_pressure"),
        F.percent_rank().over(w7).alias("corr_humidity_pressure_percent_rank"),
    )
)

scored = (
    enriched_device_readings
    .withColumn("last_known_pressure", F.last("pressure", ignorenulls=True).over(w4))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("prev_status_bits_rank", F.lag("status_bits_rank", 1).over(w9))
    .withColumn("running_status_bits_rank_bucket", F.sum("status_bits_rank_bucket").over(w4))
)

valid_device_readings = (
    scored
    .filter(F.col("running_temp_c_avg_3_prior") == 110)
    .select(
        "id",
        "humidity",
        "pressure",
        "corr_humidity_pressure",
        "status_bits_rank",
        "pressure_tier",
        "corr_humidity_pressure_whole",
        "running_pressure",
        "corr_humidity_pressure_percent_rank",
        "prev_status_bits_rank",
        "running_status_bits_rank_bucket",
        F.when(F.col("running_pressure") >= 4134, "High").when(F.col("running_pressure") >= 1983, "Medium").otherwise("Low").alias("running_pressure_tier"),
    )
)

with_windows = (
    valid_device_readings
    .withColumn("status_bits_rank_avg_6", F.avg("status_bits_rank").over(w10))
    .withColumn("cumulative_running_status_bits_rank_bucket", F.sum("running_status_bits_rank_bucket").over(w11))
)

result = (
    with_windows
    .select(
        "id",
        "pressure",
        "corr_humidity_pressure",
        "status_bits_rank",
        "pressure_tier",
        "corr_humidity_pressure_whole",
        "running_pressure",
        "running_status_bits_rank_bucket",
        "status_bits_rank_avg_6",
    )
)
