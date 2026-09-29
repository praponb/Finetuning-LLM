from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("status_bits", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 02:00:00").cast("timestamp"))
    .withColumn("device_code_upper", F.upper("device_code"))
)

scored = (
    valid
    .withColumn("humidity_avg_5", F.avg("humidity").over(w_grp))
    .withColumn("cumulative_status_bits", F.sum("status_bits").over(w2))
    .withColumn("device_code_sum_humidity", F.sum("humidity").over(w_running))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "device_code_upper",
    )
)

latest = (
    deduped
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
    )
)
