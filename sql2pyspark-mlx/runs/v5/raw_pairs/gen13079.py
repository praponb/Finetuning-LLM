from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure_times_status_bits", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy("pressure_times_status_bits")
w_part = Window.orderBy("pressure_avg_2", "id")

base = (
    df_device_readings
    .filter(F.col("temp_c") >= 35)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("pressure") * F.col("status_bits")).alias("pressure_times_status_bits"),
    )
)

metrics = (
    base
    .select(
        "id",
        "humidity",
        "firmware",
        F.avg("pressure").over(w).alias("pressure_avg_2"),
        F.ntile(4).over(w2).alias("status_bits_bucket"),
        F.percent_rank().over(w3).alias("pressure_times_status_bits_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("status_bits_bucket") <= 4)
    .select(
        "id",
        "humidity",
        "firmware",
        "pressure_avg_2",
        "pressure_times_status_bits_percent_rank",
        F.round("humidity", 2).alias("humidity_rounded"),
    )
)

with_windows = (
    valid
    .filter(F.col("pressure_times_status_bits_percent_rank") >= 0.68)
    .select(
        "*",
        F.lead("pressure_avg_2").over(w_part).alias("next_pressure_avg_2"),
        F.lag("humidity").over(w_part).alias("prev_humidity"),
        F.lead("humidity").over(w_part).alias("next_humidity"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("pressure_avg_2") >= 1026, "HIGH").otherwise("LOW").alias("pressure_avg_2_tier"),
        "seq_no",
        F.when(F.col("prev_humidity") >= 65, "PREMIUM").otherwise("STANDARD").alias("prev_humidity_tier"),
    )
    .agg(
        F.percentile_approx("next_humidity", 0.5).alias("next_humidity_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("humidity_rounded", 0.9).alias("humidity_rounded_p90"),
        F.avg("next_pressure_avg_2").alias("avg_next_pressure_avg_2"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    summary
    .select(
        "pressure_avg_2_tier",
        "seq_no",
        "prev_humidity_tier",
        "distinct_ids",
        "distinct_ids_2",
        "humidity_rounded_p90",
        "avg_next_pressure_avg_2",
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first(), "humidity_rounded_p90")
)
