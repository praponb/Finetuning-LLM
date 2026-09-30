from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id").isin([7, 14, 20, 24]))
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.revenue") > 78)
    .select(F.col("p.id"), F.col("p.category"), F.col("t.revenue"))
    .orderBy("category", "revenue")
)
