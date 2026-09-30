from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("brand", "category")
    .intersect(
        df_products
        .filter(F.lower("product_name").like("%apt%"))
        .select("brand", "category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
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
        "stock_qty",
        "launch_date",
        "tags",
    )
    .orderBy(F.col("price").desc())
)
