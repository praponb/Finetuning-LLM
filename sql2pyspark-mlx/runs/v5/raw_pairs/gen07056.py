from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .filter(~F.col("d.store_name").like("%Riversid%"))
    .select("s.id", "s.store_id", "s.product_id", "s.quantity_rounded", F.col("d.city"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.id", "e.product_id", "e.city", F.col("b.launch_date"), F.col("b.brand"))
)

matched_keys = (
    enriched
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 4, 5]))
        .select("product_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("city", "brand")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("city").asc_nulls_last())
)
