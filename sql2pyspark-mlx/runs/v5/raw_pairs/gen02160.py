from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy(F.col("next_temp_c").asc_nulls_first())
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("next_temp_c_avg_3").desc_nulls_first(), "id")
w_part = Window.orderBy("next_temp_c_avg_3", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp")) & (F.col("temp_c") <= 24))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.floor("status_bits").alias("status_bits_whole"),
    )
)

scored = (
    filtered
    .filter(F.col("status_bits_whole") == 40)
    .withColumn("prev_status_bits", F.lag("status_bits").over(w))
    .withColumn("next_temp_c", F.lead("temp_c").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("running_status_bits", F.sum("status_bits").over(w2))
)

enriched = (
    scored
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "firmware",
        "prev_status_bits",
        "next_temp_c",
        "running_status_bits",
        F.percent_rank().over(w_recent).alias("next_temp_c_percent_rank"),
        F.avg("next_temp_c").over(w4).alias("next_temp_c_avg_3"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "humidity",
        "firmware",
        "next_temp_c",
        "next_temp_c_percent_rank",
        "next_temp_c_avg_3",
    )
)

scored_device_readings = (
    latest
    .withColumn("prev_next_temp_c_percent_rank", F.lag("next_temp_c_percent_rank").over(w_part))
    .withColumn("next_next_temp_c_percent_rank", F.lead("next_temp_c_percent_rank").over(w_part))
    .withColumn("prev_humidity", F.lag("humidity").over(w_part))
    .withColumn("row_seq", F.row_number().over(w_part))
)

result = (
    scored_device_readings
    .select(
        "id",
        "humidity",
        "firmware",
        "next_temp_c_percent_rank",
        "next_temp_c_avg_3",
        "prev_next_temp_c_percent_rank",
        "next_next_temp_c_percent_rank",
        "prev_humidity",
        "row_seq",
    )
)
