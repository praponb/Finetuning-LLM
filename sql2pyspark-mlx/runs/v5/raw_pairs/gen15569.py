from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") < 310)
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("unit_price_count").desc(), F.col("avg_unit_price").desc())
)
