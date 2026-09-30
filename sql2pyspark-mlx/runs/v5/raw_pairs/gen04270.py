from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .filter(F.col("avg_unit_price") > 190)
    .orderBy(F.col("category").desc(), F.col("avg_unit_price").desc(), "brand")
    .limit(20)
)
