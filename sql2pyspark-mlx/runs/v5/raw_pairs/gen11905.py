from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "prev_reading_ts",
        "session_start_flag",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-03|DV-02)$"), "GROUP_B").when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "CORE").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy(F.col("device_code").desc())
)
