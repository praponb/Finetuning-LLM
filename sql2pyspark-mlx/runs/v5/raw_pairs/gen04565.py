from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("p.stock_qty").between(102, 399))
    .groupBy(F.col("s.city"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
)
