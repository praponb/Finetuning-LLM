from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.round((F.col("oi.unit_price") * 0.9), 2).alias("unit_price_adjusted"),
    )
    .orderBy("supplier_id")
)
