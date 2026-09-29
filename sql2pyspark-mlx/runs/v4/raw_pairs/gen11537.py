from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.supplier_id").isin([6, 16, 36]))
    .groupBy(F.col("o.channel"))
    .agg(F.countDistinct(F.col("oi.unit_price")).alias("unique_products"))
    .filter(F.col("unique_products") > 2)
)
