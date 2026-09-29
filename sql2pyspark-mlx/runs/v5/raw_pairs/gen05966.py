from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "right")
    .groupBy(
        F.when(F.col("products.cost") >= 610, "high").when(F.col("products.cost") >= 100, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum((F.col("order_items.unit_price") * F.col("order_items.quantity"))).alias("revenue"),
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
    )
    .orderBy(F.col("unit_price_count").desc(), F.col("revenue").desc())
)
