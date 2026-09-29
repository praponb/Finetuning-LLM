from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("unit_price") >= 360)
    .groupBy("product_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("ps.avg_unit_price"),
    )
)
