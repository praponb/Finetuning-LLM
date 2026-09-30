from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("store_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([1, 5]))
        .select("store_id", "id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "product_id", "last_restock")
    .orderBy(F.col("last_restock").asc_nulls_first())
)
