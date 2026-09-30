from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
    )
    .filter(F.col("unique_products") >= 3)
)
