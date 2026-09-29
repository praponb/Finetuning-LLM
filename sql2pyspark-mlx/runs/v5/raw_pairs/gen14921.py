from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("value", "promo")
w4 = Window.partitionBy("value").orderBy(F.col("price").desc())
w5 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_first())
w6 = Window.partitionBy("promo").orderBy(F.col("qty_rank").asc_nulls_last())

exploded = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

exploded_carts = (
    exploded
    .select(
        "id",
        "product_id",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

metrics = (
    exploded_carts
    .select(
        "*",
        F.min("price").over(w).alias("value_min_price"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share_2"),
        F.percent_rank().over(w4).alias("price_percent_rank"),
        F.rank().over(w5).alias("qty_rank"),
    )
)

metrics_carts = (
    metrics
    .filter(F.col("qty") <= 5)
    .select(
        "id",
        "price",
        "promo",
        "value",
        "price_share_2",
        "price_percent_rank",
        "qty_rank",
        F.count("price_percent_rank").over(w2).alias("promo_count_price_percent_rank"),
        F.rank().over(w6).alias("qty_rank_rank"),
    )
)

result = (
    metrics_carts
    .select(
        "value",
        F.col("price").alias("PRICE_VAL"),
        "price_share_2",
        F.col("price_percent_rank").alias("PRICE_PERCENT_RANK_VAL"),
    )
    .unpivot(
        ids=["value"],
        values=["PRICE_VAL", "price_share_2", "PRICE_PERCENT_RANK_VAL"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
