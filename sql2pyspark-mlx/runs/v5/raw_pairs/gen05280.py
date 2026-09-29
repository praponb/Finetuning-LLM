from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.category").isin(["Home", "Fashion"]))
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
