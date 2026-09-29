from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("cost").between(340, 390))
    .select(
        "supplier_id",
        "product_name",
        "price",
        F.substring("product_name", 1, 3).alias("product_name_prefix"),
    )
)
