from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w_grp = Window.partitionBy("promo").orderBy(F.col("price").asc_nulls_first())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.dense_rank().over(w_grp).alias("price_dense_rank"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isin([10, 30, 39]))
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("promo").isNotNull())
    .withColumn("is_welcome5", F.when(F.col("promo") == "WELCOME5", 1).otherwise(0))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("UNKNOWN")))
)

totals = (
    base
    .groupBy(
        F.when(F.col("price_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("price_dense_rank_tier"),
    )
    .agg(
        F.countDistinct("promo").alias("distinct_promos"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("price_dense_rank")).otherwise(0)).alias("spring10_price_dense_rank"),
        F.count("*").alias("row_count"),
    )
)

result = (
    totals
    .select(
        "price_dense_rank_tier",
        "distinct_promos",
        "spring10_price_dense_rank",
        "row_count",
    )
    .orderBy(F.col("row_count").desc(), "spring10_price_dense_rank")
)
