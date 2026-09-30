from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("price").asc_nulls_last())
w2 = Window.orderBy("qty")
w3 = Window.partitionBy("element")
w4 = Window.orderBy(F.col("price").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.rank().over(w).alias("price_rank"),
        F.rank().over(w2).alias("qty_rank"),
        F.count("qty").over(w3).alias("element_count_qty"),
        F.rank().over(w4).alias("price_rank_2"),
    )
)

cleaned = (
    enriched
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 17:00:00"))) & F.col("customer_id").isNotNull() & F.col("price").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "qty",
        "price",
        "promo",
        "element",
        "price_rank",
        "qty_rank",
        "element_count_qty",
        "price_rank_2",
        F.round("price_rank", 2).alias("price_rank_rounded"),
    )
)

base = (
    cleaned
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & F.col("element").isNotNull() & F.col("price_rank_2").isNull())
    .select(
        "*",
        F.month("created_at").alias("created_at_month"),
        F.coalesce("element", F.lit("UNKNOWN")).alias("element_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "created_at",
        "qty",
        "promo",
        "price_rank",
        "qty_rank",
        "element_count_qty",
        "price_rank_2",
        "price_rank_rounded",
        F.when(F.col("promo").rlike("^(WELCOME5|SPRING10)$"), "PRIORITY").when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("GROUP_B").alias("promo_zone"),
    )
    .orderBy(F.col("price_rank_2").desc())
)
