from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id").isin([1, 2, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("s.city"),
        F.datediff(F.current_date(), F.col("p.launch_date")).alias("days_since_launch_date"),
    )
)
