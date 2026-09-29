from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("pt.unit_price_count"),
        F.col("pt.unique_products"),
    )
    .orderBy("unique_products")
)
