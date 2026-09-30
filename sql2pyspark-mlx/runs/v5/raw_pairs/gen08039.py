from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.partitionBy("region")
w4 = Window.orderBy(F.col("sqft_rank").desc_nulls_first())
w5 = Window.partitionBy("region", "city")
w6 = Window.partitionBy("region", "city").orderBy(F.col("city_min_sqft").desc())

base = (
    df_stores
    .filter(F.col("opened_date").between("2024-08-15", "2024-12-06"))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

kept_keys = (
    base
    .select("region", "city")
    .intersectAll(
        df_stores
        .filter(F.col("region").isin(["West", "Central"]))
        .select("region", "city")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "opened_date_month_end",
        "region_clean",
        F.min("sqft").over(w).alias("city_min_sqft"),
        F.rank().over(w2).alias("sqft_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

with_windows = (
    scored
    .select(
        "sqft",
        "sqft_rank",
        "sqft_share",
        F.percent_rank().over(w4).alias("sqft_rank_percent_rank"),
        F.round((F.col("sqft_rank") / F.sum("sqft_rank").over(w5)), 4).alias("sqft_rank_share"),
        F.rank().over(w6).alias("city_min_sqft_rank"),
    )
)

result = (
    with_windows
    .select(
        "sqft_rank",
        "sqft_rank_percent_rank",
        "sqft_rank_share",
        "city_min_sqft_rank",
    )
    .orderBy(F.col("city_min_sqft_rank").desc_nulls_last(), "sqft_rank")
)
