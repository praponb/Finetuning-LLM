from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("price_scaled").asc_nulls_first())
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("price_scaled").desc_nulls_first())
w4 = Window.partitionBy("promo").orderBy(F.col("price_scaled").asc_nulls_last())
w5 = Window.partitionBy("promo").orderBy("price_scaled_dense_rank_3")

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "coupon_codes", F.col("line.price"), F.col("line.promo"))
)

base = (
    exploded
    .filter(F.col("price") <= 80)
    .select(
        "*",
        F.round((F.col("price") * 0.9), 2).alias("price_scaled"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_clean"),
    )
)

scored = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("price_scaled_dense_rank"),
        F.dense_rank().over(w).alias("price_scaled_dense_rank_2"),
        F.max("price_scaled").over(w2).alias("promo_max_price_scaled"),
        F.dense_rank().over(w3).alias("price_scaled_dense_rank_3"),
        F.cume_dist().over(w4).alias("price_scaled_cume_dist"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "price",
        "price_scaled_dense_rank_2",
        "promo_max_price_scaled",
        "price_scaled_cume_dist",
        F.max("price_scaled_dense_rank_3").over(w2).alias("promo_max_price_scaled_dense_rank_3"),
        F.sum("promo_max_price_scaled").over(w2).alias("promo_sum_promo_max_price_scaled"),
        F.rank().over(w5).alias("price_scaled_dense_rank_3_rank"),
        F.round((F.col("price_scaled_dense_rank_3") / F.sum("price_scaled_dense_rank_3").over(w2)), 4).alias("price_scaled_dense_rank_3_share"),
        F.max("price_scaled_dense_rank_3").over(w2).alias("promo_max_price_scaled_dense_rank_3_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "price_scaled_dense_rank_2",
        "promo_max_price_scaled",
        "price_scaled_cume_dist",
        "price_scaled_dense_rank_3_rank",
        "price_scaled_dense_rank_3_share",
        "promo_max_price_scaled_dense_rank_3_2",
    )
    .orderBy(F.col("price_scaled_cume_dist").desc_nulls_first())
)
