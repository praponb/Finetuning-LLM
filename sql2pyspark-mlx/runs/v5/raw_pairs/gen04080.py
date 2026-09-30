from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "brand",
        "price",
        F.array_join("tags", ", ").alias("tags_text"),
        F.greatest("cost", "price").alias("greatest_cost_price"),
    )
    .orderBy(F.col("id").desc())
)
