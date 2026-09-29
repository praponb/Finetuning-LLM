from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("brand", "id")
    .intersect(
        df_products
        .filter(F.col("cost").between(640, 710))
        .select("brand", "id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("product_name", "brand", "price", "stock_qty", "tags")
    .orderBy("price")
)
