from pyspark.sql import functions as F

excluded_products = df_products.filter(F.array_contains("tags", "bestseller")).select("id")

result = (
    df_order_items.alias("oi")
    .join(excluded_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("oi.product_id").isNotNull() | F.lit(excluded_products.isEmpty()))
    .select(F.col("oi.id"), F.col("oi.product_id"))
)
