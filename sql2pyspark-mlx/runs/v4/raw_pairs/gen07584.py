from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("o.discount") >= 0.29)
    .groupBy(F.col("o.status"), F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .orderBy("avg_unit_price", "status", "order_quarter")
    .limit(5)
)
