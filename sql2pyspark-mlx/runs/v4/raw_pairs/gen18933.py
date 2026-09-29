from pyspark.sql import functions as F

src = (
    df_inventory
    .filter((F.col("store_id") == 5) & F.col("quantity").between(156, 170) & (F.col("last_restock") > "2025-01-31"))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    product_id_summary
    .filter(F.col("inventory_count") < 7)
    .select("product_id", "inventory_count")
    .orderBy(F.col("inventory_count").desc())
)
