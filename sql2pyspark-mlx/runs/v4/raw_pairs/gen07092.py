from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("is_new_session").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("pressure", "id")

base = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("temp_c") > 31))
    .withColumn("greatest_humidity_temp_c", F.greatest("humidity", "temp_c"))
)

with_gaps = (
    base
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

latest = (
    sessionized
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_prev = latest.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id_2", F.sum("is_new_session").over(w2))

latest_device_readings = (
    sessions_device_readings
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_device_readings_2 = (
    latest_device_readings
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

stats = (
    latest_device_readings_2
    .groupBy("burst_id")
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("is_new_burst")).otherwise(0)).alias("dv_03_is_new_burst"),
        F.min("burst_id").alias("min_burst_id"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("is_new_session"))).alias("dv_02_max_is_new_session"),
        F.min("reading_ts").alias("first_seen"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
)

result = (
    stats
    .select(
        "burst_id",
        "dv_03_is_new_burst",
        "first_seen",
        "avg_pressure",
        "num_device_codes",
    )
    .orderBy(F.col("num_device_codes").desc())
)
