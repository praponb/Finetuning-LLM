from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first(), "id")
w2 = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_label", F.date_format("reading_ts", "yyyy-MM-dd"))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.min("humidity").over(w2).alias("device_code_min_humidity"),
        F.corr("humidity", "temp_c").over(w2).alias("corr_humidity_temp_c"),
    )
)

valid = (
    scored
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 04:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "reading_ts_label",
        "latest_rank",
        "device_code_min_humidity",
        "corr_humidity_temp_c",
        F.to_date("reading_ts").alias("reading_ts_date"),
        F.when(F.col("corr_humidity_temp_c") >= 0.41, "High").when(F.col("corr_humidity_temp_c") >= -0.44, "Medium").otherwise("Low").alias("corr_humidity_temp_c_level"),
    )
)

cleaned = (
    valid
    .filter(F.hour("reading_ts").between(7, 15))
    .select("*", F.initcap(F.trim("device_code")).alias("device_code_title"))
)

result = (
    cleaned
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "latest_rank",
        "device_code_min_humidity",
        "corr_humidity_temp_c",
    )
    .orderBy("device_code_min_humidity")
)
