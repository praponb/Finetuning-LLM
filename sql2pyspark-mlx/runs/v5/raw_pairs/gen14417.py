from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("B%") | (F.col("stock_qty") >= 128))
    .select(
        "stock_qty",
        F.array_contains("tags", "premium").alias("is_premium"),
        (F.col("cost") + F.col("price")).alias("margin"),
    )
)
