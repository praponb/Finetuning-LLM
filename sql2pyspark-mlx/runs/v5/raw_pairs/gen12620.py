from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").desc_nulls_last(), "id")
w_recent = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("temp_c") >= 37.5, "High").when(F.col("temp_c") >= 26.3, "Medium").otherwise("Low").alias("temp_c_tier"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

deduped = (
    sessions_device_readings
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        "temp_c_tier",
        F.max("new_group_flag").over(w_recent).alias("running_max_new_group_flag"),
        F.sum("new_group_flag").over(w_recent).alias("running_sum_new_group_flag"),
        F.avg("new_group_flag").over(w_recent).alias("running_avg_new_group_flag"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "firmware",
        "running_max_new_group_flag",
        "running_avg_new_group_flag",
    )
    .orderBy(F.col("device_code").desc_nulls_first())
)
