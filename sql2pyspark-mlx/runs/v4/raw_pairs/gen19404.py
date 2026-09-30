from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("temp_c_filled", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("temp_c_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("temp_c_rounded").asc_nulls_last())
w8 = Window.orderBy(F.col("burst_id").desc(), "id")
w9 = Window.orderBy("burst_id", "id").rowsBetween(-2, Window.currentRow)

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w4))

base = (
    sessionized
    .filter((F.col("reading_ts") > F.lit("2025-09-01 14:00:00").cast("timestamp")) & (F.col("burst_id") == 7))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "seq_no",
        "is_new_session",
        "burst_id",
        F.dayofmonth("reading_ts").alias("reading_ts_day"),
        F.greatest("humidity", "seq_no").alias("greatest_humidity_seq_no"),
    )
)

base_device_readings = (
    base
    .filter(F.col("status_bits") < 8)
    .select(
        "id",
        "temp_c",
        "status_bits",
        "is_new_session",
        "burst_id",
        "reading_ts_day",
        F.when(F.col("reading_ts_day") >= 86, "High").when(F.col("reading_ts_day") >= 14, "Medium").otherwise("Low").alias("reading_ts_day_tier"),
    )
)

filtered = (
    base_device_readings
    .filter(F.col("reading_ts_day").between(42, 51))
    .select(
        "id",
        "status_bits",
        "burst_id",
        "reading_ts_day_tier",
        F.coalesce("temp_c", F.lit(0)).alias("temp_c_filled"),
        F.round("temp_c", 2).alias("temp_c_rounded"),
    )
)

scored = (
    filtered
    .withColumn("temp_c_rounded_avg_3", F.avg("temp_c_rounded").over(w5))
    .withColumn("first_known_temp_c_filled", F.first("temp_c_filled", ignorenulls=True).over(w6))
    .withColumn("temp_c_rounded_dense_rank", F.dense_rank().over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("burst_id_avg_2", F.avg("burst_id").over(w9))
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_bits") >= 11, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
        F.when(F.col("temp_c_filled") >= 63, "HIGH").otherwise("LOW").alias("temp_c_filled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("burst_id_avg_2", [0.25, 0.5, 0.75]).alias("burst_id_avg_2_quartiles"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.percentile_approx("temp_c_filled", 0.95).alias("temp_c_filled_p95"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
