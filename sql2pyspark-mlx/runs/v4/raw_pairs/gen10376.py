from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.orderBy("sqft")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region")
w5 = Window.orderBy(F.col("sqft").desc())
w6 = Window.orderBy(F.col("city_min_sqft").desc())
w7 = Window.partitionBy("region").orderBy(F.col("sqft").desc())

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2024-01-20").cast("date"))
    .withColumn("city_region_key", F.concat_ws(",", "city", "region"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    deduped
    .select("id")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") > "2023-11-01")
        .select("id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("sqft_rank", F.rank().over(w2))
    .withColumn("city_min_sqft", F.min("sqft").over(w3))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w5))
)

remaining_keys_2 = (
    metrics
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["North", "West"]))
        .select("id", "city")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.dense_rank().over(w6).alias("city_min_sqft_dense_rank"),
        F.dense_rank().over(w7).alias("sqft_dense_rank_2"),
        F.max("sqft_dense_rank").over(w3).alias("city_max_sqft_dense_rank"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("city_max_sqft_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("city_max_sqft_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("row_count_2"),
        F.countDistinct("city").alias("distinct_cities"),
    )
)
