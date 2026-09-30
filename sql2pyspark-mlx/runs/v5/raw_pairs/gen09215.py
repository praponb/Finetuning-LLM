from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.brand") == "Initech")
    .groupBy(F.col("p.id"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("id").desc())
)
