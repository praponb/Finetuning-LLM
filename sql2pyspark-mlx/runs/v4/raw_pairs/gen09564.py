from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts_date").desc(), "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("latest_rank", "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("first_known_pressure").desc())
w8 = Window.partitionBy("device_code").orderBy("session_id", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("humidity_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("reading_ts_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("session_id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .withColumn("reading_ts_date", F.to_date("reading_ts"))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.min("pressure").over(w2).alias("device_code_min_pressure"),
        F.first("pressure", ignorenulls=True).over(w3).alias("first_known_pressure"),
    )
)

with_prev = scored.withColumn("prev_reading_ts", F.lag("reading_ts").over(w4))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w5))

filtered = (
    sessionized
    .filter(F.col("pressure") > 1025)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "reading_ts_date",
        "latest_rank",
        "first_known_pressure",
        "prev_reading_ts",
        "is_new_burst",
        "session_id",
        F.col("humidity").cast("int").alias("humidity_int"),
    )
)

with_windows = (
    filtered
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("first_known_pressure_cume_dist", F.cume_dist().over(w7))
    .withColumn("humidity_int_avg_5", F.avg("humidity_int").over(w8))
)

result = (
    with_windows
    .select(
        "*",
        F.sum("status_bits").over(w9).alias("cumulative_status_bits"),
        F.sum("temp_c").over(w10).alias("running_temp_c"),
        F.dense_rank().over(w11).alias("session_id_dense_rank"),
    )
)
