from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country")
w_grp_2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "country").orderBy("rating_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "country")
w5 = Window.partitionBy("country").orderBy("rating_scaled")
w6 = Window.partitionBy("country").orderBy(F.col("city_stddev_samp_rating").desc())
w7 = Window.partitionBy("city").orderBy("country_max_rating_scaled")
w8 = Window.orderBy(F.col("first_known_rating_scaled").desc_nulls_last())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") * 0.9), 2).alias("rating_scaled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.max("rating_scaled").over(w_grp).alias("country_max_rating_scaled"),
        F.stddev_samp("rating").over(w_grp_2).alias("city_stddev_samp_rating"),
        F.first("rating_scaled", ignorenulls=True).over(w3).alias("first_known_rating_scaled"),
    )
)

matched_keys = (
    scored
    .select("country", "id")
    .intersect(
        df_suppliers
        .filter(F.col("country") != "TH")
        .select("country", "id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= "2024-10-01"), "left_anti")
)

scored_suppliers = (
    eligible
    .select(
        "*",
        F.sum("rating_scaled").over(w4).alias("city_sum_rating_scaled"),
        F.cume_dist().over(w5).alias("rating_scaled_cume_dist"),
        F.rank().over(w6).alias("city_stddev_samp_rating_rank"),
        F.percent_rank().over(w7).alias("country_max_rating_scaled_percent_rank"),
    )
)

not_flagged = (
    scored_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.size(F.col("c.tags")) >= 2), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.rank().over(w8).alias("first_known_rating_scaled_rank"),
        F.min("city_stddev_samp_rating_rank").over(w_grp).alias("country_min_city_stddev_samp_rating_rank"),
        F.max("rating_scaled_cume_dist").over(w_grp_2).alias("city_max_rating_scaled_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "id",
        "supplier_name",
        "city",
        "country_max_rating_scaled",
        "city_stddev_samp_rating",
        "first_known_rating_scaled",
        "rating_scaled_cume_dist",
        "city_stddev_samp_rating_rank",
        "country_max_rating_scaled_percent_rank",
    )
    .orderBy("city_stddev_samp_rating_rank")
)
