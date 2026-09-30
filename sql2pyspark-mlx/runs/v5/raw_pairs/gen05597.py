from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.orderBy(F.col("sqft").desc())

valid = (
    df_stores
    .filter(F.col("sqft").between(5421, 5496))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.ntile(3).over(w).alias("sqft_bucket"),
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("sqft").between(4594, 6447))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(~F.col("region").eqNullSafe(F.lit("North")))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft_bucket",
        (F.col("sqft_bucket") + F.col("sqft_dense_rank")).alias("sqft_bucket_plus_sqft_dense_rank"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("sqft_bucket_plus_sqft_dense_rank") >= 75, "PREMIUM").otherwise("STANDARD").alias("sqft_bucket_plus_sqft_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("sqft_bucket_plus_sqft_dense_rank", [0.25, 0.5, 0.75]).alias("sqft_bucket_plus_sqft_dense_rank_quartiles"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("row_count").desc_nulls_last())
)
