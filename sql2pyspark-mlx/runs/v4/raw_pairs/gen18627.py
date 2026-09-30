from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first(), "id")
w2 = Window.partitionBy("device_code").orderBy("humidity")
w3 = Window.orderBy("pressure", "id")
w4 = Window.orderBy("temp_c", "id")
w5 = Window.orderBy("reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w8 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .withColumn("is_dv_04", F.when(F.col("device_code") == "DV-04", 1).otherwise(0))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked_device_readings = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    ranked_device_readings
    .select(
        "*",
        F.lead("is_dv_04", 1).over(w3).alias("next_is_dv_04"),
        F.lead("pressure", 2).over(w4).alias("next_pressure"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

filtered = (
    scored
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.round((F.col("row_seq") / F.col("temp_c")), 2).alias("row_seq_per_temp_c"),
    )
)

with_prev = filtered.withColumn("prev_reading_ts", F.lag("reading_ts").over(w7))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w8))

remaining_keys = (
    sessions_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") != "1.0.3")
        .select("firmware", "device_code")
    )
)

verified = (
    sessions_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "is_dv_04",
        "next_is_dv_04",
        "next_pressure",
        "row_seq",
    )
)
