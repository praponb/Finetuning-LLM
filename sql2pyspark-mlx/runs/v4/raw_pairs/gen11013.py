from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("supplier_id", "category")
    .exceptAll(
        df_products
        .filter(F.col("stock_qty") == 249)
        .select("supplier_id", "category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
    )
    .orderBy(F.col("brand").desc_nulls_first())
)
