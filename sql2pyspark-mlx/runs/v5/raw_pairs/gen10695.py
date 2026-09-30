from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 13)
    .select(
        "id",
        "tags",
        "brand",
        "category",
        F.initcap(F.trim("category")).alias("category_title"),
    )
)
