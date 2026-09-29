from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

result = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)
