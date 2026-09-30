from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("price").alias("sum_product_price"))

remaining_keys = (
    df_products
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("cost") > 650)
        .select("id", "supplier_id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category").rlike("^(Fashion|Grocery)$"), "GROUP_B").otherwise("GROUP_A").alias("category_zone"),
        F.col("sum_product_price"),
    )
    .orderBy("launch_date")
)
