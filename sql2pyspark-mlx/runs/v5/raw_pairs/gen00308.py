from pyspark.sql import functions as F

quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

remaining_keys = (
    df_store_sales
    .select("id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 3, 5]))
        .select("id", "product_id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("sale_date").asc_nulls_last())
)
