from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.year("launch_date") == 2023)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.lower("category").alias("category_lower"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.units").between(33, 37)), "left_anti")
)
