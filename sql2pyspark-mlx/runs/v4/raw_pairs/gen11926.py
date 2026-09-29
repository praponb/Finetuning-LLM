from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.orderBy(F.col("opened_date_dayofweek").desc(), "id")
w3 = Window.orderBy(F.col("sqft").desc_nulls_last())
w4 = Window.partitionBy("city")
w5 = Window.orderBy(F.col("sqft").desc_nulls_first())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("sqft").between(6811, 8475))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.date_sub("opened_date", 52).alias("opened_date_minus_52d"),
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.ntile(4).over(w2).alias("opened_date_dayofweek_bucket"),
    )
)

kept_keys = (
    scored
    .select("id")
    .intersectAll(
        df_stores
        .filter(~F.col("region").isin(["West", "Central"]))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "*",
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
        F.round((F.col("prev_sqft") / F.sum("prev_sqft").over(w4)), 4).alias("prev_sqft_share"),
        F.rank().over(w5).alias("sqft_rank"),
    )
)
