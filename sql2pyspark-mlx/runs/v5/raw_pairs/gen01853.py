from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        F.ceil("temp_c").alias("temp_c_whole"),
    )
)

result = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "device_code", "humidity", "humidity_filled", "temp_c_whole")
)
