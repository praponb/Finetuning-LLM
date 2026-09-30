from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("device_code")
w5 = Window.orderBy("humidity", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & ~F.col("device_code").isin(["DV-02", "DV-04"]))
    .withColumn("reading_ts_month", F.month("reading_ts"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .withColumn("reading_ts_month_count_30m", F.count("reading_ts_month").over(w2))
    .withColumn("reading_ts_month_avg_2", F.avg("reading_ts_month").over(w_ordered))
    .withColumn("pressure_share", F.round((F.col("pressure") / F.sum("pressure").over(w4)), 4))
    .withColumn("pressure_avg_6", F.avg("pressure").over(w5))
)

kept_keys = (
    with_windows
    .select("firmware")
    .subtract(
        df_device_readings
        .filter(F.col("temp_c").between(27, 30))
        .select("firmware")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "reading_ts_month",
        "rank_in_group",
        "reading_ts_month_count_30m",
        "reading_ts_month_avg_2",
        "pressure_avg_6",
        F.when(F.col("device_code").isin(["DV-01", "DV-04"]), "Core").otherwise("Other").alias("device_code_group"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("humidity") >= 68, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("reading_ts").alias("first_reading_ts"),
    )
    .orderBy(F.col("humidity_tier").desc_nulls_first())
)
