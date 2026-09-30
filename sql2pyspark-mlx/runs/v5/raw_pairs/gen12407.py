from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([3, 4, 6]))
        .select("product_id", "store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "product_id", "last_restock")
    .orderBy(F.col("id").asc_nulls_last())
)
