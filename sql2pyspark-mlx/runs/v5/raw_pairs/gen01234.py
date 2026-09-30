from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("firmware_length_2", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("firmware_length_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("last7_count_firmware_length_2", "id")
w4 = Window.orderBy(F.col("last7_avg_firmware_length_2").desc(), "id")
w5 = Window.orderBy("last7_avg_firmware_length_2", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("last7_min_firmware_length_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("status_bits") >= 7)
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        F.length("firmware").alias("firmware_length"),
    )
)

valid = (
    base
    .filter(F.col("firmware").isin(["1.1.0", "1.0.3"]))
    .select(
        "id",
        "pressure",
        "firmware",
        "firmware_length",
        F.length("firmware").alias("firmware_length_2"),
    )
)

scored = (
    valid
    .withColumn("last7_count_firmware_length_2", F.count("firmware_length_2").over(w_recent))
    .withColumn("last7_min_firmware_length_2", F.min("firmware_length_2").over(w_recent))
    .withColumn("last7_max_firmware_length_2", F.max("firmware_length_2").over(w_recent))
    .withColumn("last7_avg_firmware_length_2", F.avg("firmware_length_2").over(w_recent))
)

enriched = (
    scored
    .select(
        "id",
        "firmware",
        "firmware_length",
        F.first("firmware_length_2", ignorenulls=True).over(w2).alias("first_known_firmware_length_2"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.avg("firmware_length").over(w5).alias("firmware_length_avg_2"),
        F.sum("last7_avg_firmware_length_2").over(w6).alias("cumulative_last7_avg_firmware_length_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("event_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("first_known_firmware_length_2").alias("avg_first_known_firmware_length_2"),
        F.sum("firmware_length").alias("total_firmware_length"),
    )
)
