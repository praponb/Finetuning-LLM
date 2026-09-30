from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 14:00:00")))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.when(F.col("status_bits") >= 12, "High").when(F.col("status_bits") >= 2, "Medium").otherwise("Low").alias("status_bits_tier"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
