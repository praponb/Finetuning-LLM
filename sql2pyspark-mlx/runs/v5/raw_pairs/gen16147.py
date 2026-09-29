from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("session_id", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("rn").desc(), "id")

latest = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    latest
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w3))

with_windows = (
    sessions_device_readings
    .filter(F.to_date("reading_ts") >= "2024-01-01")
    .select(
        "*",
        F.count("temp_c").over(w4).alias("running_count_temp_c"),
        F.avg("temp_c").over(w4).alias("running_avg_temp_c"),
        F.min("temp_c").over(w4).alias("running_min_temp_c"),
    )
)

metrics = (
    with_windows
    .withColumn("next_is_new_session", F.lead("is_new_session").over(w5))
    .withColumn("prev_is_new_session", F.lag("is_new_session").over(w5))
    .withColumn("next_running_count_temp_c", F.lead("running_count_temp_c").over(w5))
    .withColumn("prev_running_count_temp_c", F.lag("running_count_temp_c").over(w5))
    .withColumn("row_seq", F.row_number().over(w5))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select("id", "device_code", "pressure", "running_min_temp_c", "running_avg_temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "running_min_temp_c", "running_avg_temp_c"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
