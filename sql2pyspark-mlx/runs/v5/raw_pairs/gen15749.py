from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("i.quantity").between(99, 136))
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
    .orderBy(F.col("quantity").desc(), F.col("launch_date").desc())
)
