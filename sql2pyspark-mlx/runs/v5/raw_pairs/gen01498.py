from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.orderBy("temp_c", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("event_seq").desc(), "id")

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(11, 14))
    .withColumn("pressure_plus_temp_c", (F.col("pressure") + F.col("temp_c")))
)

scored = (
    cleaned
    .select(
        "*",
        F.corr("status_bits", "humidity").over(w).alias("corr_status_bits_humidity"),
        F.lag("humidity", 1).over(w2).alias("prev_humidity"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("humidity") / F.sum("humidity").over(w)), 4).alias("humidity_share"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "corr_status_bits_humidity",
        "prev_humidity",
        "humidity_share",
        "latest_rank",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

filtered_device_readings = (
    filtered
    .filter(F.col("days_since_reading_ts") <= 57)
    .select(
        "*",
        F.col("days_since_reading_ts").cast("double").alias("days_since_reading_ts_str"),
        (F.col("humidity_share") + F.col("days_since_reading_ts")).alias("humidity_share_plus_days_since_reading_ts"),
    )
)

result = (
    filtered_device_readings
    .groupBy(
        F.when(F.col("prev_humidity") >= 77, "LARGE").otherwise("SMALL").alias("prev_humidity_tier"),
        F.when(F.col("corr_status_bits_humidity") >= 0.62, "PREMIUM").otherwise("STANDARD").alias("corr_status_bits_humidity_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
    )
)
