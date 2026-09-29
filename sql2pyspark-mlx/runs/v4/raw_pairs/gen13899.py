from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([2, 4, 6, 8]))
        .select("product_id", "id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "product_id", "quantity", "last_restock")
