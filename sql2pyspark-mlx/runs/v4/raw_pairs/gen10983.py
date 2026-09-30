from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_day", F.dayofmonth("reading_ts"))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

remaining_keys = (
    sessions_device_readings
    .select("device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c").isNotNull())
        .select("device_code")
    )
)

reconciled = (
    sessions_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "row_num",
        "is_new_burst",
        "session_id",
    )
    .orderBy("status_bits", "is_new_burst")
)
