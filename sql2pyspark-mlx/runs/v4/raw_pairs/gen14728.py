from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.product_id").isin([1, 4, 10]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
)
