from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull() & (F.col("cost") > 450))
    .select(
        "id",
        "tags",
        F.concat_ws("-", "product_name", "brand").alias("product_name_brand_key"),
    )
)
