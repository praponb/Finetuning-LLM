from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.partitionBy("city")

base = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-05-05").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

kept_keys = (
    base
    .select("id", "city")
    .intersectAll(
        df_stores
        .filter(F.col("sqft") != 6170)
        .select("id", "city")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("city") == "Berlin")
    .select("*", F.rank().over(w).alias("sqft_rank"))
)

with_windows_stores = (
    with_windows
    .select(
        "id",
        "store_name",
        "sqft_filled",
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
        F.count("sqft_rank").over(w3).alias("city_count_sqft_rank"),
        F.round((F.col("sqft_rank") / F.sum("sqft_rank").over(w3)), 4).alias("sqft_rank_share"),
    )
)

cleaned = (
    with_windows_stores
    .filter(F.col("sqft_rank_share").isNull())
    .select(
        "*",
        F.round((F.col("sqft_percent_rank") * 1.15), 2).alias("sqft_percent_rank_scaled"),
        (F.col("sqft_filled") * F.col("sqft_percent_rank")).alias("sqft_filled_times_sqft_percent_rank"),
    )
)

matched_keys = (
    cleaned
    .select("id")
    .subtract(
        df_stores
        .filter(F.month("opened_date").isin([9, 10, 11]))
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "store_name",
        "sqft_filled",
        "sqft_percent_rank",
        "city_count_sqft_rank",
        "sqft_rank_share",
        "sqft_percent_rank_scaled",
        "sqft_filled_times_sqft_percent_rank",
    )
)
