from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

scored = (
    df_device_readings
    .withColumn("pressure_cume_dist", F.cume_dist().over(w))
    .withColumn("status_bits_bucket", F.ntile(4).over(w2))
)

result = (
    scored
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "pressure_cume_dist",
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("firmware").desc())
    .limit(20)
)
