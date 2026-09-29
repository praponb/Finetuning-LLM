from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("humidity")
w5 = Window.orderBy("status_bits_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

latest = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

filtered = (
    sessionized
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("humidity") >= 43))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "rank_in_group",
        "session_start_flag",
        "session_id",
        F.col("status_bits").cast("double").alias("status_bits_str"),
    )
)

enriched = (
    filtered
    .withColumn("humidity_cume_dist", F.cume_dist().over(w4))
    .withColumn("cumulative_humidity", F.sum("humidity").over(w5))
    .withColumn("status_bits_str_sum_2h", F.sum("status_bits_str").over(w6))
)

result = (
    enriched
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "rank_in_group",
        "session_start_flag",
        "session_id",
    )
)
