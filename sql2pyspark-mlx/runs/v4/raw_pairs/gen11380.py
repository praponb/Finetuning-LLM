from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("product_id", "store_id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([2, 5, 6]))
        .select("product_id", "store_id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "store_id", "product_id", "quantity", "last_restock")
    .orderBy(F.col("id").desc_nulls_first())
)
