from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts_day", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w4 = Window.partitionBy("device_code")
w5 = Window.orderBy(F.col("status_bits").desc(), "id")
w_ordered = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("status_bits_avg_5_prior").desc())
w8 = Window.partitionBy("device_code").orderBy(F.col("status_bits_bucket").asc_nulls_first(), "id")
w9 = Window.partitionBy("device_code").orderBy("device_code_avg_status_bits_avg_5_prior", "id")
w10 = Window.orderBy("status_bits_bucket", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("temp_c") <= 34)
    .select("*", F.col("reading_ts").cast("date").alias("reading_ts_day"))
)

scored = (
    base
    .filter(F.col("device_code") == "DV-01")
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "reading_ts_day",
        F.avg("status_bits").over(w).alias("status_bits_avg_5_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.rank().over(w3).alias("humidity_rank"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w4)), 4).alias("temp_c_share"),
        F.ntile(4).over(w5).alias("status_bits_bucket"),
    )
)

scored_device_readings = (
    scored
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "seq_no",
        "status_bits_bucket",
        F.avg("status_bits_avg_5_prior").over(w4).alias("device_code_avg_status_bits_avg_5_prior"),
        F.last("pressure", ignorenulls=True).over(w_ordered).alias("last_known_pressure"),
        F.rank().over(w7).alias("status_bits_avg_5_prior_rank"),
    )
)

latest = (
    scored_device_readings
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "device_code",
        "status_bits_bucket",
        F.lag("last_known_pressure").over(w9).alias("prev_last_known_pressure"),
        F.lead("seq_no").over(w9).alias("next_seq_no"),
        F.lag("seq_no").over(w9).alias("prev_seq_no"),
        F.row_number().over(w9).alias("event_seq"),
        F.avg("seq_no").over(w10).alias("seq_no_avg_3"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("next_seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("next_seq_no_tier"),
    )
    .agg(
        F.percentile_approx("seq_no_avg_3", 0.5).alias("seq_no_avg_3_p50"),
        F.round(F.sum("prev_last_known_pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("prev_last_known_pressure_per_dv_03"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("prev_last_known_pressure_per_dv_03").desc_nulls_last())
)
