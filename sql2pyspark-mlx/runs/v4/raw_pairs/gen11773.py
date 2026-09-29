from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "session_start_flag",
        "burst_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-03)$"), "OTHER_ZONE").when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "CORE").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy(F.col("session_start_flag").desc())
)
