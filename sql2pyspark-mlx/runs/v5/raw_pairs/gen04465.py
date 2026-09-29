from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("is_new_session").desc(), "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.coalesce("temp_c", F.lit(0)).alias("temp_c_filled"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

deduped = (
    sessions_device_readings
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "humidity", "pressure", "temp_c_filled", "is_new_session", "burst_id")
)

cleaned = (
    deduped
    .filter(F.col("humidity") > 53)
    .select(
        "id",
        "humidity",
        "pressure",
        "temp_c_filled",
        "is_new_session",
        "burst_id",
        (F.col("temp_c_filled") + F.col("humidity")).alias("temp_c_filled_plus_humidity"),
    )
)

kept_keys = (
    cleaned
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("firmware").isin(["2.0.1", "1.1.0", "1.0.3"]))
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .groupBy("burst_id")
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("avg_is_new_session") > 0)
)

result = (
    daily
    .select("burst_id", "avg_is_new_session", "distinct_ids")
    .orderBy(F.col("avg_is_new_session").desc_nulls_first())
)
