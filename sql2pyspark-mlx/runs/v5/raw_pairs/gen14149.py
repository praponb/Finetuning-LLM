from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

kept_keys = (
    df_store_sales
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") <= 2900)
        .select("store_id", "id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("units").asc_nulls_last(), F.col("product_id").desc())
)
