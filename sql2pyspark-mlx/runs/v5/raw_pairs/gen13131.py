from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("order_id") == 40)
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.revenue") > 43)
    .select(
        F.col("p.price"),
        F.col("p.cost"),
        F.col("t.revenue"),
        F.col("t.avg_quantity"),
    )
    .orderBy(F.col("price").desc(), F.col("revenue").desc())
)
