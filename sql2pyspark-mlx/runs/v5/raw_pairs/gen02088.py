from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id") == 2)
    .select(
        F.col("p.price"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.coalesce(F.col("p.cost"), F.lit(0)).alias("cost_filled"),
    )
)
