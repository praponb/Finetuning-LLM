from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("supplier_id", "id")
    .intersect(
        df_products
        .filter(F.size("tags") >= 2)
        .select("supplier_id", "id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category").rlike("^(Sports|Books)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy("launch_date")
)
