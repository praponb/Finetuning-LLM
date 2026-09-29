from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("adjusted_status_bits", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("status_bits", "id")
w_hist = Window.orderBy(F.col("humidity_scaled").desc(), "id")
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

filtered = (
    df_device_readings
    .filter(~F.col("firmware").isin(["2.0.1", "1.0.3"]))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.round((F.col("status_bits") * 1.07), 2).alias("adjusted_status_bits"),
    )
)

base = (
    filtered
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.round((F.col("humidity") * 0.9), 2).alias("humidity_scaled"),
        F.floor("pressure").alias("pressure_whole"),
    )
)

scored = (
    base
    .withColumn("humidity_avg_5_prior", F.avg("humidity").over(w))
    .withColumn("status_bits_bucket", F.ntile(10).over(w2))
)

base_device_readings = (
    scored
    .filter(F.col("status_bits_bucket") != 5)
    .select("*", F.coalesce("humidity", F.lit(0)).alias("humidity_filled"))
)

metrics = (
    base_device_readings
    .select(
        "*",
        F.row_number().over(w_hist).alias("row_seq"),
        F.max("status_bits").over(w4).alias("status_bits_max_30m"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(11, 18))
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    reconciled
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 38.6, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["hour_window", "row_seq_tier", "temp_c_tier"], ["hour_window", "row_seq_tier"], ["hour_window"], []],
        "hour_window",
        "row_seq_tier",
        "temp_c_tier",
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("status_bits_max_30m", [0.25, 0.5, 0.75]).alias("status_bits_max_30m_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    summary
    .select(
        "row_seq_tier",
        "temp_c_tier",
        "duration_min",
        "distinct_ids",
        "status_bits_max_30m_quartiles",
        "distinct_ids_2",
    )
)
