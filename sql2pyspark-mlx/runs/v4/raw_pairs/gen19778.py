from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "category",
        F.substring("category", 1, 4).alias("category_prefix"),
    )
)
