from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

remaining_keys = (
    df_inventory
    .select("id")
    .subtract(
        df_inventory
        .filter(F.month("last_restock").isin([1, 4, 9]))
        .select("id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("sum_inventory_quantity").desc())
)
