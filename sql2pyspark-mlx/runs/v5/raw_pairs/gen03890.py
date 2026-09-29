from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("device_code") == "DV-02")
    .withColumn("clean_device_code", F.trim("device_code"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

flagged = (
    islands
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "session_no",
        F.sum("is_new_burst").over(w3).alias("burst_id"),
    )
)

deduped = (
    sessions_device_readings
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "reading_ts", "humidity", "pressure")
)

filtered = (
    deduped
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 07:00:00")))
    .select(
        "*",
        (F.col("pressure") + F.col("humidity")).alias("pressure_plus_humidity"),
    )
)

result = (
    filtered
    .select("id", "humidity", "pressure", "pressure_plus_humidity")
    .orderBy(F.col("pressure_plus_humidity").desc_nulls_first(), "id")
)
