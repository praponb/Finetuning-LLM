from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(F.col("p.id"), F.col("p.category"), F.col("pt.avg_unit_price"))
    .orderBy(F.col("id").desc())
)
