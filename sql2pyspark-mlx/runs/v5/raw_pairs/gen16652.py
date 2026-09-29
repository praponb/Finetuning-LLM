from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("reading_ts", "id")
w4 = Window.orderBy("temp_c", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("prev_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.orderBy("running_pressure", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy("event_seq", "id")
w_recent = Window.orderBy("last_known_event_seq_sum_15m", "id")
w11 = Window.orderBy("event_seq", "id").rowsBetween(-6, -1)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp")) & F.col("temp_c").between(28, 32))
    .withColumn("pressure_per_temp_c", F.round((F.col("pressure") / F.col("temp_c")), 2))
)

enriched = (
    valid
    .filter(F.col("temp_c") <= 27)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.lead("humidity").over(w_grp).alias("next_humidity"),
        F.lag("pressure").over(w_grp).alias("prev_pressure"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.sum("pressure").over(w2).alias("running_pressure"),
        F.lead("event_seq", 1).over(w_part).alias("next_event_seq"),
    )
)

scored_device_readings = (
    scored
    .select(
        "*",
        F.avg("next_humidity").over(w4).alias("next_humidity_avg_5_prior"),
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
        F.last("event_seq", ignorenulls=True).over(w6).alias("last_known_event_seq_2"),
        F.avg("status_bits").over(w7).alias("status_bits_avg_15m"),
        F.sum("pressure").over(w5).alias("running_pressure_2"),
    )
)

metrics = (
    scored_device_readings
    .select(
        "id",
        "status_bits",
        "firmware",
        "prev_pressure",
        "event_seq",
        "next_event_seq",
        "last_known_event_seq_2",
        "running_pressure_2",
        F.avg("status_bits").over(w8).alias("status_bits_avg_5"),
        F.sum("last_known_event_seq").over(w7).alias("last_known_event_seq_sum_15m"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "status_bits",
        "event_seq",
        "next_event_seq",
        "last_known_event_seq_2",
        "last_known_event_seq_sum_15m",
        F.ntile(10).over(w_running).alias("event_seq_bucket"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("status_bits").over(w11).alias("status_bits_avg_6_prior"),
    )
)

valid_device_readings = (
    metrics_device_readings
    .filter(F.col("last_known_event_seq_2").between(3, 6))
    .withColumn("status_bits_per_last_known_event_seq_2", F.round((F.col("status_bits") / F.col("last_known_event_seq_2")), 2))
)

stats = (
    valid_device_readings
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["event_seq_tier", "seq_no"], ["event_seq_tier"], []],
        "event_seq_tier",
        "seq_no",
    )
    .agg(
        F.round(F.avg("event_seq_bucket"), 2).alias("avg_event_seq_bucket"),
        F.count("*").alias("num_rows"),
        F.grouping("event_seq_tier").alias("gid"),
    )
    .filter(F.col("avg_event_seq_bucket") > 0)
)

result = stats.select("event_seq_tier", "seq_no", "avg_event_seq_bucket", "gid")
