from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("last_restock").between("2023-11-01", "2024-02-09") & (F.col("last_restock") > "2025-03-04") & F.col("product_id").isin([5, 7]))
)

inventory_stats = (
    src
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    inventory_stats
    .orderBy("product_id")
    .limit(5)
)
