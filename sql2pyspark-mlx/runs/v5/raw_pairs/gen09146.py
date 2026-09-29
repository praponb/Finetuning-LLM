from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .select(
        "id",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "session_start_flag",
        "burst_id",
    )
    .orderBy(F.col("session_start_flag").desc())
)
