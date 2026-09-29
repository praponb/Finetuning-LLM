from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        "product_name",
        "supplier_id",
        F.lower("brand").alias("brand_lower"),
    )
)
