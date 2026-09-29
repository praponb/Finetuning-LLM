from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount") > 0.27)
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("unit_price_count").desc(), F.col("row_count").desc(), "brand")
    .limit(1)
)
