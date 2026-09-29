from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .groupBy(
        F.col("p.category"),
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
    )
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy("launch_month", F.col("category").desc())
)
