from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(F.col("p.category"), F.col("p.supplier_id"), F.col("pt.max_quantity"))
)
