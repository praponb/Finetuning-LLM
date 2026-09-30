from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Yog%"))
    .select(
        "id",
        "category",
        "product_name",
        F.concat_ws("-", "category", "brand").alias("category_brand_key"),
    )
    .orderBy("id")
    .limit(3)
)
