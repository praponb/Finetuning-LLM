from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.length(F.col("s.supplier_name")) > 12)
    .groupBy(F.col("p.category"))
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
)
