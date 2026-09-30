from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .filter(F.col("oibp.total_unit_price") > 2000)
    .select(F.col("p.id"), F.col("p.product_name"), F.col("oibp.total_unit_price"))
)
