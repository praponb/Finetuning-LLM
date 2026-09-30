from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("element", "promo")
w3 = Window.partitionBy("promo", "element")
w4 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.partitionBy("element").orderBy(F.col("price").asc_nulls_last())

exploded = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

flattened = (
    exploded
    .select(
        "created_at",
        "product_id",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

base = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-01 10:00:00").cast("timestamp"))
    .select(
        "created_at",
        "product_id",
        "price",
        "promo",
        "element",
        F.upper("promo").alias("promo_upper"),
    )
)

enriched = (
    base
    .filter(F.col("promo") != "VIP")
    .select(
        "*",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.round((F.col("product_id") / F.sum("product_id").over(w3)), 4).alias("product_id_share_2"),
    )
)

metrics = (
    enriched
    .select(
        "product_id",
        "promo_upper",
        "product_id_share",
        F.avg("price_share").over(w4).alias("price_share_avg_1h"),
        F.percent_rank().over(w_grp).alias("price_percent_rank"),
        F.stddev_samp("product_id_share_2").over(w2).alias("element_stddev_samp_product_id_share_2"),
    )
)

result = (
    metrics
    .select(
        "product_id",
        "promo_upper",
        "product_id_share",
        "price_share_avg_1h",
        "price_percent_rank",
    )
    .orderBy(F.col("price_share_avg_1h").asc_nulls_first())
)
