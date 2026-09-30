from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 10:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") * F.col("humidity")).alias("status_bits_times_humidity"),
    )
)

result = (
    cleaned
    .withColumn("device_code_count_status_bits", F.count("status_bits").over(w))
    .withColumn("next_pressure", F.lead("pressure", 1).over(w2))
)
