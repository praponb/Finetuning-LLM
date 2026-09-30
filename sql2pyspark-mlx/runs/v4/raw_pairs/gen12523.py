from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.min_quantity") >= 210)
    .select(F.col("p.id"), F.col("p.category"), F.col("pt.min_quantity"))
    .orderBy("category", "id")
    .limit(20)
)
