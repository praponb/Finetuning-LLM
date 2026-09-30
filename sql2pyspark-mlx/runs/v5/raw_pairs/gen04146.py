from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("price").desc())
w2 = Window.partitionBy("promo")
w_part = Window.partitionBy("promo").orderBy(F.col("price_share_whole").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

metrics = (
    flattened
    .filter(F.col("promo") != "BUNDLE")
    .select(
        "id",
        "product_id",
        "price",
        "promo",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
    )
)

valid = (
    metrics
    .filter(F.col("price_share") < 0.58)
    .select(
        "product_id",
        "price",
        "promo",
        (F.col("price_share") + F.col("price_dense_rank")).alias("price_share_plus_price_dense_rank"),
        F.floor("price_share").alias("price_share_whole"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.avg("price_share_plus_price_dense_rank").over(w2).alias("promo_avg_price_share_plus_price_dense_rank"),
        F.cume_dist().over(w_part).alias("price_share_whole_cume_dist"),
    )
)

result = (
    scored
    .select(
        "product_id",
        "price",
        "promo",
        "price_share_plus_price_dense_rank",
        "price_share_whole",
        "promo_filled",
        "promo_avg_price_share_plus_price_dense_rank",
        "price_share_whole_cume_dist",
        F.when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "EXTENDED").when(F.col("promo").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("GROUP_B").alias("promo_zone"),
    )
    .orderBy(F.col("price_share_whole_cume_dist").asc_nulls_last())
)
