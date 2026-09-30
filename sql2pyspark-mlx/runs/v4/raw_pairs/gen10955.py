from pyspark.sql import functions as F

result = (
    df_products
    .select("category")
    .intersectAll(
        df_products
        .filter(F.lower(F.trim("product_name")) == "smart watch")
        .select("category")
    )
)
