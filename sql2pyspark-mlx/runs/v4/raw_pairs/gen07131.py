from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2024-09-01")
    .select("*", F.to_date("reading_ts").alias("reading_ts_date"))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    latest
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

deduped = (
    islands
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

remaining_keys = (
    sessions_device_readings
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.col("firmware") != "2.0.1")
        .select("id")
    )
)

kept_rows = (
    sessions_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "reading_ts",
        "humidity",
        "status_bits",
        "reading_ts_date",
        "row_num",
        "is_new_session",
        "latest_rank",
    )
    .orderBy(F.col("is_new_session").desc_nulls_first())
)
