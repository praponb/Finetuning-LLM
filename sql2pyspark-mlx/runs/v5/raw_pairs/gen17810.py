from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_first())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country", "city")
w5 = Window.partitionBy("city").orderBy(F.col("rating").desc())
w6 = Window.orderBy(F.col("rating_percent_rank").desc_nulls_last())
w7 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w8 = Window.partitionBy("city").orderBy(F.col("rating_percent_rank").desc())

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isin([3, 29, 33])), "left_semi")
)

kept_keys = (
    not_flagged
    .select("country", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("city") == "Tokyo")
        .select("country", "city")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("city").isNull())
    .select(
        "supplier_name",
        "country",
        "city",
        "rating",
        F.rank().over(w).alias("rating_rank"),
        F.percent_rank().over(w2).alias("rating_percent_rank"),
        F.stddev_samp("rating").over(w3).alias("city_stddev_samp_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
        F.count("rating").over(w3).alias("city_count_rating"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.rank().over(w5).alias("rating_rank_2"),
        F.cume_dist().over(w6).alias("rating_percent_rank_cume_dist"),
        F.max("rating").over(w3).alias("city_max_rating"),
        F.rank().over(w7).alias("rating_rank_3"),
    )
)

result = (
    with_windows
    .select(
        "supplier_name",
        "country",
        "city",
        "rating_rank",
        "city_stddev_samp_rating",
        "rating_share",
        "city_count_rating",
        F.dense_rank().over(w8).alias("rating_percent_rank_dense_rank"),
        F.stddev_samp("rating").over(w3).alias("city_stddev_samp_rating_2"),
    )
)
