from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("p.stock_qty").between(121, 306) & (F.col("p.brand") != "Umbrella"))
    .select(
        F.col("i.product_id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
    )
)
