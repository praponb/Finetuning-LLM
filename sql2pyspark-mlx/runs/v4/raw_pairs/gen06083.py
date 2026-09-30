from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "supplier_id",
        "category",
        "price",
        F.floor("cost").alias("cost_whole"),
    )
)
