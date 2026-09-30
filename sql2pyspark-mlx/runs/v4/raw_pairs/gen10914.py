from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.partitionBy("device_code")
w_grp = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w6 = Window.partitionBy("device_code").orderBy("running_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("humidity") <= 44)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
    )
)

enriched = (
    filtered
    .withColumn("running_humidity", F.sum("humidity").over(w_recent))
    .withColumn("next_humidity", F.lead("humidity", 1, 0).over(w2))
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w3))
    .withColumn("humidity_max_15m", F.max("humidity").over(w_grp))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched_device_readings = (
    latest
    .select(
        "*",
        F.sum("corr_status_bits_humidity").over(w_recent).alias("cumulative_corr_status_bits_humidity"),
        F.sum("humidity_filled").over(w6).alias("running_humidity_filled"),
    )
)

result = (
    enriched_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        "humidity_filled",
        "running_humidity",
        "corr_status_bits_humidity",
        "humidity_max_15m",
        "running_humidity_filled",
    )
    .orderBy(F.col("running_humidity").asc_nulls_last(), "reading_ts")
)
