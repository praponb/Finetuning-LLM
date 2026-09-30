from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "cost",
        "product_name",
        "stock_qty",
        F.initcap(F.trim("category")).alias("category_title"),
        F.array_join("tags", ", ").alias("tags_text"),
    )
)
