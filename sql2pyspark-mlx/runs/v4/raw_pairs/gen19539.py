from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy("id")
)
