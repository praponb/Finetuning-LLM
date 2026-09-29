from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(~F.col("p.category").isin(["Home", "Books", "Fashion"]))
    .groupBy(F.col("p.category"))
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
    )
)
