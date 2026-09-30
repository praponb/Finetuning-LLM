from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "brand",
        "launch_date",
        "tags",
        F.substring("product_name", 1, 3).alias("product_name_prefix"),
    )
)
