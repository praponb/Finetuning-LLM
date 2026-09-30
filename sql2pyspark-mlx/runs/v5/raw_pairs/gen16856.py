from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

with_windows = (
    df_device_readings
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w))
    .withColumn("pressure_avg_15m", F.avg("pressure").over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "pressure_avg_15m",
    )
    .orderBy(F.col("pressure_avg_15m").asc_nulls_first())
)
