from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.max_unit_price") >= 160)
    .select(F.col("p.cost"), F.col("p.brand"), F.col("pt.max_unit_price"))
)
