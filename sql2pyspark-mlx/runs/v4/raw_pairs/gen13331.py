from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("store_id", "product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity") > 190)
        .select("store_id", "product_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "store_id", "quantity")
    .orderBy(F.col("id").desc())
)
