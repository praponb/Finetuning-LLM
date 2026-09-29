from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.stddev_unit_price") >= 37)
    .select(F.col("p.cost"), F.col("ps.stddev_unit_price"), F.col("ps.stddev_quantity"))
)
