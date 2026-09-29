from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.orderBy("pressure", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("island_id", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").desc_nulls_last())
w7 = Window.partitionBy("device_code").orderBy(F.col("next_temp_c").desc_nulls_last())

filtered = (
    df_device_readings
    .filter(F.hour("reading_ts").between(11, 19))
    .withColumn("firmware_group", F.when(F.col("firmware").isin(["1.1.0", "1.0.3"]), "Priority").otherwise("Other"))
)

scored = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.row_number().over(w).alias("row_seq"),
        F.avg("pressure").over(w2).alias("pressure_avg_5"),
    )
)

valid = (
    scored
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .withColumn("status_bits_times_row_seq", (F.col("status_bits") * F.col("row_seq")))
)

with_gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w4))

filtered_device_readings = (
    sessionized
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("status_bits_num", F.col("status_bits").cast("double"))
    .withColumn("humidity_per_status_bits_times_row_seq", (F.col("humidity") / F.nullif("status_bits_times_row_seq", F.lit(0))))
)

scored_device_readings = (
    filtered_device_readings
    .filter(F.col("island_id") != 3)
    .withColumn("prev_humidity", F.lag("humidity").over(w_grp))
    .withColumn("next_humidity", F.lead("humidity").over(w_grp))
    .withColumn("next_temp_c", F.lead("temp_c").over(w_grp))
    .withColumn("prev_temp_c", F.lag("temp_c").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("new_group_flag_percent_rank", F.percent_rank().over(w6))
)

latest = (
    scored_device_readings
    .withColumn("rank_in_group", F.rank().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "status_bits_times_row_seq",
        "new_group_flag",
        "island_id",
        "humidity_per_status_bits_times_row_seq",
    )
)
