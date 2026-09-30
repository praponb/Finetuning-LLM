from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.orderBy(F.col("humidity").asc_nulls_first())
w3 = Window.orderBy(F.col("humidity").asc_nulls_last())
w4 = Window.orderBy(F.col("pressure").asc_nulls_first())
w5 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("humidity") < 56)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.when(F.col("device_code").isin(["DV-03", "DV-01"]), "Group A").otherwise("Other").alias("device_code_group"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "reading_ts", "humidity", "pressure", "firmware", "device_code_group")
)

remaining_keys = (
    latest
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.col("temp_c") > 36)
        .select("id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("reading_ts") < "2023-10-01")
    .select(
        "*",
        F.cume_dist().over(w2).alias("humidity_cume_dist"),
        F.percent_rank().over(w3).alias("humidity_percent_rank"),
        F.rank().over(w4).alias("pressure_rank"),
    )
)

scored = (
    with_windows
    .select(
        "reading_ts",
        "firmware",
        "device_code_group",
        F.max("pressure_rank").over(w5).alias("pressure_rank_max_2h"),
        F.sum("humidity_percent_rank").over(w6).alias("humidity_percent_rank_sum_15m"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.percentile_approx("humidity_percent_rank_sum_15m", [0.25, 0.5, 0.75]).alias("humidity_percent_rank_sum_15m_quartiles"),
        F.max("reading_ts").alias("ended_at"),
        F.round(F.avg("humidity_percent_rank_sum_15m"), 2).alias("avg_humidity_percent_rank_sum_15m"),
    )
    .orderBy("hour_window")
)
