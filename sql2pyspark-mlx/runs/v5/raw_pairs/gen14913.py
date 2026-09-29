from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price").isNotNull() & F.array_contains("tags", "new")) | (F.col("launch_date") <= "2025-01-31"))
    .select(
        "id",
        "price",
        "launch_date",
        "category",
        F.array_contains("tags", "eco").alias("is_eco"),
    )
)
