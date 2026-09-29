from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last(), "id")
w5 = Window.partitionBy("device_code").orderBy("rank_in_group", "id")
w6 = Window.orderBy("last_known_temp_c", "id")
w7 = Window.partitionBy("device_code").orderBy("status_bits_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("pressure_minus_temp_c", (F.col("pressure") - F.col("temp_c")))
)

enriched = (
    base
    .filter(F.col("temp_c").between(30, 32))
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        F.last("temp_c", ignorenulls=True).over(w).alias("last_known_temp_c"),
        F.avg("temp_c").over(w2).alias("temp_c_avg_6"),
        F.lag("humidity", 1).over(w3).alias("prev_humidity"),
    )
)

valid = (
    enriched
    .filter(F.col("humidity") >= 62)
    .select("*", F.floor("status_bits").alias("status_bits_whole"))
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("prev_status_bits", F.lag("status_bits", 1).over(w6))
    .withColumn("cumulative_pressure", F.sum("pressure").over(w7))
)

result = (
    scored
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "temp_c_avg_6",
        "prev_humidity",
        "status_bits_whole",
        "prev_status_bits",
    )
    .orderBy(F.col("prev_humidity").desc())
)
