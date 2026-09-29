from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("inventory_count") > 4)
    .orderBy(F.col("total_quantity").desc(), "inventory_count")
)
