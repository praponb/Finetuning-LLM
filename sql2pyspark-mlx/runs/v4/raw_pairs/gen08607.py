from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull() & ~F.col("product_name").like("%Chair%"))
    .select(
        "id",
        "cost",
        "product_name",
        F.round((F.col("stock_qty") / F.col("price")), 2).alias("stock_qty_per_price"),
        F.array_join("tags", "|").alias("tags_text"),
    )
    .orderBy(F.col("stock_qty_per_price").desc(), "id")
    .limit(5)
)
