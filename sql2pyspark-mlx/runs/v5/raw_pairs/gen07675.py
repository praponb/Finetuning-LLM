from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_device_readings
    .select("id", "device_code", "reading_ts", "humidity", "session_id")
    .orderBy(F.col("id").asc_nulls_first())
)
