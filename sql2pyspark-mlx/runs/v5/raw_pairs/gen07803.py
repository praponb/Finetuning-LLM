from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.max_quantity") >= 3.8)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("pt.unique_orders"),
        F.col("pt.max_quantity"),
    )
)
