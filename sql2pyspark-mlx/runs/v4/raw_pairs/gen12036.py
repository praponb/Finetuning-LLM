from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.price"), F.col("t.avg_quantity"))
    .orderBy(F.col("price").desc(), F.col("avg_quantity").desc())
)
