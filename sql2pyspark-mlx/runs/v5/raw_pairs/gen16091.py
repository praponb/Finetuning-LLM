from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") >= 300)
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.revenue") > 42)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("oibp.revenue"),
        F.col("oibp.cnt"),
    )
)
