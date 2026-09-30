from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.units",
        "src.revenue",
        F.col("b.price"),
        F.col("b.category"),
    )
)
