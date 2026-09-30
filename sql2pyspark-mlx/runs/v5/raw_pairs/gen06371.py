from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("price_plus_qty").desc_nulls_first())
w2 = Window.partitionBy("promo").orderBy(F.col("price_plus_qty").desc_nulls_first())
w_grp = Window.orderBy(F.col("price_plus_qty").desc())

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

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

base = (
    exploded_carts
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "promo",
        (F.col("price") + F.col("qty")).alias("price_plus_qty"),
        F.substring("value", 1, 4).alias("value_prefix"),
    )
)

scored = (
    base
    .filter(F.col("promo") != "SPRING10")
    .select(
        "promo",
        "price_plus_qty",
        "value_prefix",
        F.cume_dist().over(w_part).alias("price_plus_qty_cume_dist"),
        F.percent_rank().over(w2).alias("price_plus_qty_percent_rank"),
        F.rank().over(w_grp).alias("price_plus_qty_rank"),
    )
)

result = (
    scored
    .select(
        "promo",
        "price_plus_qty",
        "price_plus_qty_percent_rank",
        "price_plus_qty_cume_dist",
    )
    .unpivot(
        ids=["promo"],
        values=["price_plus_qty", "price_plus_qty_percent_rank", "price_plus_qty_cume_dist"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
