from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("i.quantity") == 118) | (F.col("p.cost") >= 390))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.dayofweek(F.col("i.last_restock")).alias("last_restock_dayofweek"),
    )
    .orderBy("city")
)
