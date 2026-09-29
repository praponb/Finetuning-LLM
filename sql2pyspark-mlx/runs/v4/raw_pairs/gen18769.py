from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("pressure").desc())
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w_grp = Window.orderBy(F.col("pressure_avg_3").desc(), "id")
w_hist = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_ordered = Window.partitionBy("device_code")

enriched = (
    df_device_readings
    .withColumn("first_known_humidity", F.first("humidity", ignorenulls=True).over(w))
    .withColumn("pressure_avg_3", F.avg("pressure").over(w2))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w3))
    .withColumn("pressure_rank", F.rank().over(w4))
)

filtered = (
    enriched
    .filter(F.col("first_known_temp_c") >= 31)
    .withColumn("firmware_device_code_key", F.concat_ws(",", "firmware", "device_code"))
)

enriched_device_readings = (
    filtered
    .select(
        "*",
        F.lead("temp_c", 1).over(w5).alias("next_temp_c"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.lag("temp_c", 1).over(w_hist).alias("prev_temp_c"),
        F.min("humidity").over(w_ordered).alias("device_code_min_humidity"),
        F.sum("first_known_humidity").over(w_ordered).alias("device_code_sum_first_known_humidity"),
    )
)

valid = (
    enriched_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("first_known_humidity") <= 75))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "first_known_humidity",
        "first_known_temp_c",
        "pressure_rank",
        "firmware_device_code_key",
        "next_temp_c",
        "row_seq",
        "prev_temp_c",
        "device_code_sum_first_known_humidity",
        F.round("device_code_min_humidity", 0).alias("device_code_min_humidity_rounded"),
        F.floor("next_temp_c").alias("next_temp_c_whole"),
    )
)

result = (
    valid
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "first_known_humidity",
        "first_known_temp_c",
        "pressure_rank",
        "firmware_device_code_key",
    )
)
