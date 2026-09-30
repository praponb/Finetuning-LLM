from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("last_restock") > "2024-09-19")
        .select("product_id", "id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = verified.select("id", "store_id", "last_restock")
