from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last())
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("island_id").desc())
w8 = Window.orderBy(F.col("corr_pressure_humidity").desc(), "id")
w9 = Window.partitionBy("device_code").orderBy(F.col("prev_humidity_per_temp_c").desc_nulls_last())
w10 = Window.partitionBy("device_code").orderBy(F.col("prev_humidity_per_temp_c_rank").asc_nulls_first())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("humidity_per_temp_c", F.round((F.col("humidity") / F.col("temp_c")), 2))
)

scored = (
    filtered
    .withColumn("prev_humidity_per_temp_c", F.lag("humidity_per_temp_c", 1).over(w))
    .withColumn("corr_pressure_humidity", F.corr("pressure", "humidity").over(w2))
    .withColumn("humidity_per_temp_c_avg_3", F.avg("humidity_per_temp_c").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("temp_c_cume_dist", F.cume_dist().over(w5))
)

gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "firmware",
        "prev_humidity_per_temp_c",
        "corr_pressure_humidity",
        F.sum("is_new_burst").over(w6).alias("island_id"),
    )
)

with_windows = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "prev_humidity_per_temp_c",
        "island_id",
        F.cume_dist().over(w7).alias("island_id_cume_dist"),
        F.ntile(5).over(w8).alias("corr_pressure_humidity_bucket"),
        F.rank().over(w9).alias("prev_humidity_per_temp_c_rank"),
    )
)

kept_keys = (
    with_windows
    .select("device_code", "id")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "1.1.0")
        .select("device_code", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("island_id") < 2)
    .select(
        "device_code",
        "reading_ts",
        "prev_humidity_per_temp_c",
        "island_id_cume_dist",
        "corr_pressure_humidity_bucket",
        "prev_humidity_per_temp_c_rank",
        F.round((F.col("island_id_cume_dist") * 0.9), 2).alias("adjusted_island_id_cume_dist"),
    )
)

metrics = (
    valid
    .select(
        "device_code",
        "prev_humidity_per_temp_c",
        "island_id_cume_dist",
        "corr_pressure_humidity_bucket",
        F.rank().over(w10).alias("prev_humidity_per_temp_c_rank_rank"),
    )
)

result = (
    metrics
    .groupBy("device_code")
    .agg(
        F.percentile_approx("prev_humidity_per_temp_c_rank_rank", [0.25, 0.5, 0.75]).alias("prev_humidity_per_temp_c_rank_rank_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.percentile_approx("corr_pressure_humidity_bucket", [0.25, 0.5, 0.75]).alias("corr_pressure_humidity_bucket_quartiles"),
        F.percentile_approx("prev_humidity_per_temp_c_rank_rank", [0.25, 0.5, 0.75]).alias("prev_humidity_per_temp_c_rank_rank_quartiles_2"),
        F.count("*").alias("num_rows"),
    )
)
