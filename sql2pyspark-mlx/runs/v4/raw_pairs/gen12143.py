from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "brand",
        "launch_date",
        "stock_qty",
        F.substring("category", 1, 2).alias("category_prefix"),
    )
)
