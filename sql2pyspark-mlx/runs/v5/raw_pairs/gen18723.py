from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "*",
        F.sum("humidity").over(w).alias("last7_sum_humidity"),
        F.avg("humidity").over(w).alias("last7_avg_humidity"),
        F.max("humidity").over(w).alias("last7_max_humidity"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

base = (
    latest
    .filter(F.col("status_bits").between(5, 9))
    .withColumn("last7_sum_humidity_filled", F.coalesce("last7_sum_humidity", F.lit(0)))
    .withColumn("last7_max_humidity_filled", F.coalesce("last7_max_humidity", F.lit(0)))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w3) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w4))

base_device_readings = (
    sessions_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.when(F.col("last7_sum_humidity_filled") >= 84, "High").when(F.col("last7_sum_humidity_filled") >= 23, "Medium").otherwise("Low").alias("last7_sum_humidity_filled_band"),
    )
)

result = (
    base_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "last7_avg_humidity",
    )
    .orderBy(F.col("device_code").desc(), F.col("id").desc())
    .limit(5)
)
