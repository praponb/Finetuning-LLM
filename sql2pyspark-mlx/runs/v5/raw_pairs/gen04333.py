from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("p.stock_qty") > 370) & F.array_contains(F.col("p.tags"), "eco"))
    .select(
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("s.opened_date"),
        F.datediff(F.current_date(), F.col("i.last_restock")).alias("days_since_last_restock"),
    )
    .orderBy("product_id", "cost")
)
