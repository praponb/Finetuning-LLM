from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.lower("device_code").alias("device_code_lower"),
    )
)

with_prev = filtered.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("device_code", F.date_trunc("WEEK", "prev_reading_ts").alias("week_start"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
    )
)
