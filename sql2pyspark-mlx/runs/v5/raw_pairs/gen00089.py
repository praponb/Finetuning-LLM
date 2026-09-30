from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.to_date("reading_ts") >= "2024-01-01")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("humidity") >= 81, "High").when(F.col("humidity") >= 36, "Medium").otherwise("Low").alias("humidity_level"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "temp_c", "pressure", "firmware")
)

valid = (
    latest
    .filter(F.col("pressure") > 994)
    .select(
        "*",
        F.when(F.col("firmware").isin(["1.0.3", "1.1.0"]), "Group A").otherwise("Other").alias("firmware_group"),
    )
)

cleaned = (
    valid
    .filter(F.col("temp_c") >= 25)
    .select(
        "id",
        "pressure",
        "firmware",
        "firmware_group",
        F.round((F.col("temp_c") * 0.9), 2).alias("temp_c_scaled"),
        F.round((F.col("temp_c") / F.col("pressure")), 2).alias("temp_c_per_pressure"),
    )
)

result = (
    cleaned
    .select("id", "firmware", "firmware_group", "temp_c_scaled", "temp_c_per_pressure")
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
