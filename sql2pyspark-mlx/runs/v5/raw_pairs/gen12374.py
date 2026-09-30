from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("humidity").between(45, 58))
    .select("*", F.round((F.col("status_bits") * 1.07), 2).alias("status_bits_scaled"))
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "firmware",
        "status_bits_scaled",
    )
)
