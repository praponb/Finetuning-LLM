from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 15:00:00").cast("timestamp")) & F.col("pressure").between(1004, 1012))
    .withColumn("status_bits_times_temp_c", (F.col("status_bits") * F.col("temp_c")))
)

with_gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

latest = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "device_code",
        "temp_c",
        "status_bits_times_temp_c",
        "humidity",
        "pressure",
    )
    .unpivot(
        ["id", "device_code"],
        ["temp_c", "status_bits_times_temp_c", "humidity", "pressure"],
        "kpi",
        "value",
    )
)
