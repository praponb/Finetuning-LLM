from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("order_id").isin([4, 11, 19, 33]))
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(F.col("p.id"), F.col("p.category"), F.col("p.price"), F.col("pt.revenue"))
)
