from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element_adjusted").desc_nulls_first())
w2 = Window.orderBy(F.col("element_filled").desc())
w_hist = Window.orderBy(F.col("element_adjusted").asc_nulls_last())
w4 = Window.orderBy(F.col("element").desc_nulls_first())
w_part = Window.orderBy(F.col("element_adjusted_dense_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("element").asc_nulls_last())
w7 = Window.orderBy(F.col("element_filled_cume_dist").desc_nulls_last())
w8 = Window.orderBy("element_minus_element_adjusted_dense_rank")

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("element"))
)

filtered = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-02-08").cast("date"))
    .select(
        "*",
        F.coalesce("element", F.lit(0)).alias("element_filled"),
        F.round((F.col("element") * 1.15), 2).alias("element_adjusted"),
    )
)

scored = (
    filtered
    .filter(F.col("effective_from") > "2023-04-01")
    .select(
        "id",
        "effective_from",
        "element",
        "element_adjusted",
        F.dense_rank().over(w).alias("element_adjusted_dense_rank"),
        F.cume_dist().over(w2).alias("element_filled_cume_dist"),
        F.percent_rank().over(w_hist).alias("element_adjusted_percent_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.cume_dist().over(w4).alias("element_cume_dist"),
        F.dense_rank().over(w_part).alias("element_adjusted_dense_rank_dense_rank"),
    )
)

scored_price_tiers = (
    enriched
    .select(
        "effective_from",
        "element",
        "element_adjusted_dense_rank",
        "element_filled_cume_dist",
        F.dense_rank().over(w6).alias("element_dense_rank"),
        F.dense_rank().over(w7).alias("element_filled_cume_dist_dense_rank"),
    )
)

cleaned = (
    scored_price_tiers
    .filter(F.col("element_filled_cume_dist_dense_rank").isNull())
    .select(
        "*",
        (F.col("element_filled_cume_dist_dense_rank") - F.col("element_adjusted_dense_rank")).alias("element_filled_cume_dist_dense_rank_minus_element_adjusted_dense_rank"),
    )
)

filtered_price_tiers = (
    cleaned
    .filter(F.col("effective_from") >= F.lit("2024-05-16").cast("date"))
    .select(
        "effective_from",
        "element",
        "element_adjusted_dense_rank",
        "element_filled_cume_dist",
        "element_filled_cume_dist_dense_rank",
        "element_filled_cume_dist_dense_rank_minus_element_adjusted_dense_rank",
        (F.col("element") - F.col("element_adjusted_dense_rank")).alias("element_minus_element_adjusted_dense_rank"),
    )
)

result = (
    filtered_price_tiers
    .select(
        "element_adjusted_dense_rank",
        "element_filled_cume_dist",
        "element_filled_cume_dist_dense_rank_minus_element_adjusted_dense_rank",
        "element_minus_element_adjusted_dense_rank",
        F.cume_dist().over(w8).alias("element_minus_element_adjusted_dense_rank_cume_dist"),
    )
)
