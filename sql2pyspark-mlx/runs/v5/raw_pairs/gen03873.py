from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 380, "high").when(F.col("p.stock_qty") >= 149, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.col("p.brand"),
    )
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.count("*").alias("num_inventory"),
    )
)
