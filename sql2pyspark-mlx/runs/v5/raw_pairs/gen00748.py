from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("session_id", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.orderBy(F.col("session_start_flag").desc(), "id")

flagged = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

latest = (
    sessions_device_readings
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    latest
    .withColumn("session_id_count_30m", F.count("session_id").over(w4))
    .withColumn("session_id_avg_2", F.avg("session_id").over(w_running))
    .withColumn("session_start_flag_bucket", F.ntile(5).over(w_ordered))
)

cleaned = (
    scored
    .filter(F.col("humidity") > 36)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "session_start_flag",
        "session_id_avg_2",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

result = (
    cleaned
    .select("id", "device_code", "temp_c", "pressure", "humidity")
    .unpivot(
        ids=["id", "device_code"],
        values=["temp_c", "pressure", "humidity"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
