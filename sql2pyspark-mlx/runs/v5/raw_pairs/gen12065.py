from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("p.brand") == "Acme")
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
    .orderBy(F.col("city").desc())
    .limit(50)
)
