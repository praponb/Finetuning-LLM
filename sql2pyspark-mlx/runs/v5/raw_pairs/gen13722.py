from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.array_contains("tags", "eco") & (F.col("category") == "Electronics")) | (F.col("supplier_id") == 38))
    .select(
        "id",
        "stock_qty",
        "brand",
        F.substring("product_name", 1, 2).alias("product_name_prefix"),
    )
)
