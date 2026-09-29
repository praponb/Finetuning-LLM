from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller") & ((F.size("tags") > 1) | (F.col("price") >= 480)))
    .select(
        "id",
        "tags",
        "product_name",
        F.when(F.col("stock_qty") >= 356, "High").when(F.col("stock_qty") >= 62, "Medium").otherwise("Low").alias("stock_qty_level"),
    )
)
