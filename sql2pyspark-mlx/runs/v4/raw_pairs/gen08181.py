from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first())
w_running = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .withColumn("temp_c_dense_rank", F.dense_rank().over(w2))
    .withColumn("device_code_avg_pressure", F.avg("pressure").over(w_running))
)

with_gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w4).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w4) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w5))

filtered = (
    sessionized
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("device_code_avg_pressure") < 1003))
    .withColumn("temp_c_scaled", F.round((F.col("temp_c") * 1.07), 2))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "row_num",
    )
)
