from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("last7_max_status_bits").desc(), "id")
w3 = Window.orderBy("last7_min_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last7_max_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("temp_c_int").desc_nulls_last())
w6 = Window.orderBy("row_seq", "id")
w7 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("last_known_last7_max_status_bits").desc_nulls_first())
w9 = Window.orderBy("last_known_last7_avg_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "status_bits",
        "firmware",
        F.col("temp_c").cast("int").alias("temp_c_int"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.max("status_bits").over(w).alias("last7_max_status_bits"),
        F.avg("status_bits").over(w).alias("last7_avg_status_bits"),
        F.min("status_bits").over(w).alias("last7_min_status_bits"),
    )
)

scored = (
    enriched
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("last_known_last7_max_status_bits", F.last("last7_max_status_bits", ignorenulls=True).over(w3))
    .withColumn("last_known_last7_avg_status_bits", F.last("last7_avg_status_bits", ignorenulls=True).over(w4))
)

metrics = (
    scored
    .filter(F.col("status_bits") > 8)
    .select(
        "*",
        F.rank().over(w5).alias("temp_c_int_rank"),
        F.ntile(10).over(w6).alias("row_seq_bucket"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "*",
        F.sum("row_seq").over(w7).alias("running_row_seq"),
        F.cume_dist().over(w8).alias("last_known_last7_max_status_bits_cume_dist"),
        F.sum("status_bits").over(w9).alias("running_status_bits"),
    )
)

result = (
    metrics_device_readings
    .select(
        "id",
        "status_bits",
        "firmware",
        "temp_c_int",
        "last7_max_status_bits",
        "last7_avg_status_bits",
        "last7_min_status_bits",
        "last_known_last7_avg_status_bits",
        "temp_c_int_rank",
    )
)
