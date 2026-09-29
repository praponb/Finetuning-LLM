from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("launch_date") >= "2025-10-20") & F.col("product_name").like("%Backpac%")) | F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "cost",
        "product_name",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
    )
)
