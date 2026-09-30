from pyspark.sql import functions as F

quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

remaining_keys = (
    df_inventory
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 5, 6]))
        .select("id", "store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("store_id", "last_restock", F.col("sum_inventory_quantity"))
    .orderBy(F.col("sum_inventory_quantity").desc_nulls_last())
)
