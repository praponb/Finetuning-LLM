from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("i.store_id") == 1) & ~F.col("p.brand").isin(["Initech", "Globex"]))
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("s.sqft"),
        F.col("p.supplier_id"),
    )
)
