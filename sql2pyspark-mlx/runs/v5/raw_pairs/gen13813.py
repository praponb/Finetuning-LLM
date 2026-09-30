from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.supplier_id"), F.col("ps.last_last_restock"))
    .orderBy(F.col("supplier_id").desc())
)
