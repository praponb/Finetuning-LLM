from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("pt.min_quantity"))
)
