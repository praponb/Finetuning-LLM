from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, -1)
w4 = Window.orderBy(F.col("pressure").desc_nulls_first())
w5 = Window.orderBy(F.col("humidity").desc_nulls_last())
w6 = Window.orderBy("temp_c", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.hour("reading_ts").alias("reading_ts_hour"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.min("reading_ts_hour").over(w).alias("device_code_min_reading_ts_hour"),
        F.first("reading_ts_hour", ignorenulls=True).over(w2).alias("first_known_reading_ts_hour"),
        F.avg("status_bits").over(w3).alias("status_bits_avg_3_prior"),
        F.dense_rank().over(w4).alias("pressure_dense_rank"),
    )
)

enriched = (
    scored
    .withColumn("device_code_min_status_bits_avg_3_prior", F.min("status_bits_avg_3_prior").over(w))
    .withColumn("humidity_dense_rank", F.dense_rank().over(w5))
    .withColumn("status_bits_avg_3", F.avg("status_bits").over(w6))
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") < "2024-12-11")
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("humidity").isNotNull())
    .select(
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "reading_ts_hour",
        "device_code_min_status_bits_avg_3_prior",
        "humidity_dense_rank",
        (F.col("reading_ts_hour") * F.col("device_code_min_status_bits_avg_3_prior")).alias("reading_ts_hour_times_device_code_min_status_bits_avg_3_prior"),
    )
)

result = (
    valid
    .select("temp_c", "humidity", "pressure", "reading_ts_hour", "humidity_dense_rank")
    .orderBy(F.col("reading_ts_hour").desc_nulls_first(), "humidity")
)
