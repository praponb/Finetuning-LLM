from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("brand", "supplier_id")
    .intersectAll(
        df_products
        .filter(F.col("brand") == "Globex")
        .select("brand", "supplier_id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Office Chair|Desk Lamp)$"), "EXTENDED").otherwise("PRIORITY").alias("product_name_zone"),
    )
    .orderBy(F.col("brand").desc_nulls_last())
)
