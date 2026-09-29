from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
    .orderBy("category")
)
