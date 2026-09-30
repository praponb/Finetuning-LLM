from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("qty_tier").orderBy(F.col("spring10_count").desc())

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

cleaned = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-02 12:00:00").cast("timestamp")) & (F.col("product_id") >= 5))
    .select("*", F.least("price", "product_id").alias("least_price_product_id"))
)

valid = (
    cleaned
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & F.col("promo").isNotNull() & F.col("price").between(60, 200))
    .select(
        "id",
        "product_id",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").isin(["SPRING10", "BUNDLE"]), "Tier 1").otherwise("Other").alias("promo_group"),
        F.coalesce("promo", F.lit("Unknown")).alias("promo_filled"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("product_id").over(w).alias("promo_sum_product_id"),
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"),
        "promo",
    )
    .agg(
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_count"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.percentile_approx("product_id", [0.25, 0.5, 0.75]).alias("product_id_quartiles"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .withColumn("spring10_count_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("welcome5_count").desc())
)
