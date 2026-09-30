from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "cost",
        F.trim("product_name").alias("clean_product_name"),
    )
    .orderBy("cost")
)
