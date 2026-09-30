from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("greatest_product_id_price")
w2 = Window.partitionBy("promo").orderBy(F.col("greatest_product_id_price").asc_nulls_last())
w3 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", F.col("elem.product_id"), F.col("elem.price"), F.col("elem.promo"))
)

cleaned = (
    flattened
    .filter(F.col("product_id") <= 9)
    .withColumn("promo_lower", F.lower("promo"))
)

filtered = (
    cleaned
    .filter(F.col("product_id").between(6, 7))
    .select(
        "*",
        F.coalesce("promo", F.lit("Unknown")).alias("promo_filled"),
        F.greatest("product_id", "price").alias("greatest_product_id_price"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_filled_2"),
    )
)

with_windows = (
    filtered
    .filter(F.col("price") <= 230)
    .select(
        "*",
        F.percent_rank().over(w).alias("greatest_product_id_price_percent_rank"),
        F.percent_rank().over(w2).alias("greatest_product_id_price_percent_rank_2"),
        F.stddev_samp("greatest_product_id_price").over(w3).alias("promo_stddev_samp_greatest_product_id_price"),
    )
)

valid = (
    with_windows
    .filter(F.col("promo").isNotNull() & (F.col("price") > 210))
    .withColumn("promo_upper", F.upper("promo"))
)

valid_carts = (
    valid
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "product_id",
        "price",
        "promo",
        "promo_lower",
        "promo_filled",
        "greatest_product_id_price",
        "greatest_product_id_price_percent_rank",
        "greatest_product_id_price_percent_rank_2",
        "promo_upper",
        F.length("promo").alias("promo_length"),
        F.coalesce("promo", F.lit("DIRECT")).alias("resolved_promo"),
    )
)

result = (
    valid_carts
    .groupBy("promo")
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.countDistinct("promo").alias("distinct_promos"),
    )
    .filter(F.col("num_promos") > 1)
    .orderBy("promo")
)
