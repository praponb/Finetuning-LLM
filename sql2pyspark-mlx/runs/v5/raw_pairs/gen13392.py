from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
    .orderBy(F.col("revenue").desc(), F.col("max_quantity").desc())
)
