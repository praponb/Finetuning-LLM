from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 447, "high").when(F.col("p.stock_qty") >= 95, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy("launch_month")
)
