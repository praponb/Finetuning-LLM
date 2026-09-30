from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        "supplier_id",
        "launch_date",
        F.substring("category", 1, 2).alias("category_prefix"),
    )
)
