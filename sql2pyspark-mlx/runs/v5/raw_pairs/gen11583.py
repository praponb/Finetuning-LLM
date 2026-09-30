from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("new_group_flag", "id")

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

prev_events = islands.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "new_group_flag",
        "session_no",
        F.sum("session_start_flag").over(w2).alias("session_no_2"),
    )
)

metrics = (
    islands_device_readings
    .filter(F.col("device_code") == "DV-03")
    .select(
        "*",
        F.first("temp_c", ignorenulls=True).over(w3).alias("first_known_temp_c"),
        F.lag("humidity", 2).over(w4).alias("prev_humidity"),
    )
)

cleaned = (
    metrics
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_date", F.to_date("reading_ts"))
)

result = (
    cleaned
    .select(
        "id",
        "reading_ts",
        "humidity",
        "status_bits",
        "new_group_flag",
        "session_no",
        "session_no_2",
        "first_known_temp_c",
        "reading_ts_date",
    )
    .orderBy(F.col("reading_ts").desc_nulls_last())
)
