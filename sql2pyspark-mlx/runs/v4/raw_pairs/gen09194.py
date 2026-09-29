from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price") < 190)
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.order_item_count") >= 9)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("pt.revenue"),
        F.col("pt.order_item_count"),
    )
)
