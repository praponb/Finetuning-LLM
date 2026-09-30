from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .filter(F.col("p.stock_qty").between(193, 265) & F.col("p.category").isin(["Home", "Fashion", "Grocery"]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.round((F.col("p.cost") / F.col("p.price")), 2).alias("margin"),
    )
)
