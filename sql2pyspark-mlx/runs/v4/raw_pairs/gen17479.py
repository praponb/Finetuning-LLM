from pyspark.sql import functions as F

excluded_products = df_products.filter(F.col("launch_date") > "2023-07-01").select("id")

result = (
    df_order_items.alias("oi")
    .join(excluded_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("oi.product_id").isNotNull() | F.lit(excluded_products.isEmpty()))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
