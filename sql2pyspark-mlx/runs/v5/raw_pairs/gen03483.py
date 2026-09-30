from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("quantity") <= 5)
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.revenue") >= 65)
    .select(
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("ps.max_unit_price"),
        F.col("ps.revenue"),
    )
    .orderBy(F.col("max_unit_price").desc())
)
