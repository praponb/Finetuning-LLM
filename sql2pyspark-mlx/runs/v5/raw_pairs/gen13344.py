from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

result = (
    df_device_readings
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        F.last("humidity", ignorenulls=True).over(w).alias("last_known_humidity"),
        F.count("humidity").over(w2).alias("humidity_count_30m"),
        F.ntile(3).over(w3).alias("humidity_bucket"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
