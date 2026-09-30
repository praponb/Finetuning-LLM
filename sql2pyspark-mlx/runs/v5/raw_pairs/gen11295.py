from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("reading_ts").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("island_id").desc(), "id")

scored = (
    df_device_readings
    .filter(F.col("reading_ts") < "2024-08-01")
    .withColumn("prev_status_bits", F.lag("status_bits", 1).over(w))
    .withColumn("last_known_pressure", F.last("pressure", ignorenulls=True).over(w2))
    .withColumn("running_pressure", F.sum("pressure").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

valid = (
    scored
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 03:00:00").cast("timestamp")) & (F.to_date("reading_ts") >= "2023-12-01"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_status_bits",
        "last_known_pressure",
        "running_pressure",
        F.length("firmware").alias("firmware_length"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "firmware_length",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

latest = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("island_id")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
        F.sum("firmware_length").alias("total_firmware_length"),
    )
    .filter(F.col("total_firmware_length") > 0)
    .orderBy("island_id")
)
