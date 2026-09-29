from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "launch_date",
        "tags",
        F.substring("category", 1, 2).alias("category_prefix"),
        F.round((F.col("stock_qty") / F.col("price")), 2).alias("stock_qty_per_price"),
    )
)
