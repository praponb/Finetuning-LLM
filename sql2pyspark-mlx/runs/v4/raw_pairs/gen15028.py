from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("region_length")
w_ordered = Window.partitionBy("city")
w_recent = Window.orderBy(F.col("region_length").desc_nulls_first())
w4 = Window.partitionBy("region")
w5 = Window.orderBy(F.col("sqft").desc())
w6 = Window.orderBy(F.col("sqft").desc_nulls_last())

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-14").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.length("region").alias("region_length"),
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

remaining_keys = (
    filtered
    .select("city", "id")
    .intersect(
        df_stores
        .filter(F.col("opened_date") > "2024-06-01")
        .select("city", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.percent_rank().over(w).alias("region_length_percent_rank"),
        F.stddev_samp("sqft").over(w_ordered).alias("city_stddev_samp_sqft"),
        F.percent_rank().over(w_recent).alias("region_length_percent_rank_2"),
    )
)

with_windows = (
    metrics
    .withColumn("region_max_region_length", F.max("region_length").over(w4))
    .withColumn("city_sum_region_length_percent_rank", F.sum("region_length_percent_rank").over(w_ordered))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w5))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w6))
)

result = (
    with_windows
    .groupBy("region")
    .pivot("city", ["Tokyo", "Berlin"])
    .agg(F.avg("region_length_percent_rank"))
)
