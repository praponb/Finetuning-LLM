from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("temp_c_minus_status_bits").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") - F.col("status_bits")).alias("temp_c_minus_status_bits"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.corr("humidity", "status_bits").over(w2).alias("corr_humidity_status_bits"),
        F.round((F.col("pressure") / F.sum("pressure").over(w2)), 4).alias("pressure_share"),
        F.sum("temp_c_minus_status_bits").over(w3).alias("temp_c_minus_status_bits_sum_1h"),
    )
)

deduped_device_readings = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_device_readings
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "temp_c_minus_status_bits",
        "latest_rank",
        "corr_humidity_status_bits",
        "pressure_share",
    )
)
