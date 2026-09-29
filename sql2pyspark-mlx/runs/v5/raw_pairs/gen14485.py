from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.avg_quantity") >= 29)
    .select(F.col("p.category"), F.col("pt.avg_quantity"))
    .orderBy("category")
)
