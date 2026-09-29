from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_device_readings
    .select("id", "reading_ts", "temp_c", "humidity", "session_id")
    .orderBy(F.col("humidity").desc(), "id")
    .limit(10)
)
