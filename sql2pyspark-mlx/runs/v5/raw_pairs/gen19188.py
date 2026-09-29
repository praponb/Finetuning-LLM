from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())

result = (
    df_device_readings
    .select(
        "*",
        F.avg("status_bits").over(w).alias("status_bits_avg_30m"),
        F.cume_dist().over(w2).alias("status_bits_cume_dist"),
    )
)
