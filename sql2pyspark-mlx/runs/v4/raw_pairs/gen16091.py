from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())
w_grp = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w4 = Window.partitionBy("corr_quantity_unit_price_tier").orderBy(F.col("distinct_product_ids").desc())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

kept_keys = (
    filtered
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") > 6)
        .select("id", "product_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.max("unit_price").over(w_grp).alias("product_id_max_unit_price"),
        F.count("quantity_rounded").over(w_grp).alias("product_id_count_quantity_rounded"),
        F.corr("quantity", "unit_price").over(w_grp).alias("corr_quantity_unit_price"),
        F.cume_dist().over(w3).alias("unit_price_cume_dist"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.stock_qty"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("corr_quantity_unit_price") >= 0.26, "LARGE").otherwise("SMALL").alias("corr_quantity_unit_price_tier"),
        F.when(F.col("quantity_rounded") >= 68, "LARGE").otherwise("SMALL").alias("quantity_rounded_tier"),
        "product_id",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("product_id_max_unit_price", 0.95).alias("product_id_max_unit_price_p95"),
    )
    .withColumn("distinct_product_ids_dense_rank", F.dense_rank().over(w4))
)
