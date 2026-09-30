from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy("product_id")
w3 = Window.orderBy(F.col("product_id").asc_nulls_last())
w4 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())
w5 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_first())
w6 = Window.orderBy(F.col("promo_min_product_id_dense_rank").asc_nulls_first())
w7 = Window.partitionBy("promo").orderBy("promo_count_product_id_share_2")

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", F.col("line.product_id"), F.col("line.price"), F.col("line.promo"))
)

enriched = (
    lines
    .select(
        "*",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.dense_rank().over(w2).alias("product_id_dense_rank"),
        F.percent_rank().over(w3).alias("product_id_percent_rank"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("product_id_share").between(0.43, 0.74))
    .select(
        "id",
        "product_id",
        "price",
        "promo",
        "product_id_dense_rank",
        "product_id_share_2",
        F.when(F.col("product_id_dense_rank") >= 8, "High").when(F.col("product_id_dense_rank") >= 3, "Medium").otherwise("Low").alias("product_id_dense_rank_level"),
        F.coalesce("promo", F.lit("OTHER")).alias("resolved_promo"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "product_id",
        "price",
        "promo",
        "product_id_dense_rank",
        "product_id_share_2",
        "product_id_dense_rank_level",
        "resolved_promo",
        F.percent_rank().over(w4).alias("product_id_percent_rank"),
        F.count("product_id_share_2").over(w).alias("promo_count_product_id_share_2"),
        F.min("product_id_dense_rank").over(w).alias("promo_min_product_id_dense_rank"),
    )
)

enriched_carts = (
    with_windows
    .filter(F.col("promo_count_product_id_share_2") >= 9)
    .select(
        "id",
        "product_id",
        "price",
        "promo",
        "product_id_dense_rank_level",
        "promo_count_product_id_share_2",
        F.min("product_id_percent_rank").over(w).alias("promo_min_product_id_percent_rank"),
        F.cume_dist().over(w5).alias("product_id_cume_dist"),
        F.dense_rank().over(w6).alias("promo_min_product_id_dense_rank_dense_rank"),
        F.cume_dist().over(w7).alias("promo_count_product_id_share_2_cume_dist"),
    )
)

result = (
    enriched_carts
    .select(
        "id",
        "product_id",
        "price",
        "promo",
        "product_id_dense_rank_level",
        "promo_count_product_id_share_2",
        "promo_min_product_id_percent_rank",
        "promo_count_product_id_share_2_cume_dist",
        F.when(F.col("promo").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_zone"),
    )
    .orderBy(F.col("promo_min_product_id_percent_rank").desc_nulls_first())
)
