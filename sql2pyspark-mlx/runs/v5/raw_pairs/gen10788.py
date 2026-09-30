from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("p.category").isin(["Sports", "Home"]), "priority").otherwise("other").alias("category_group"),
    )
    .agg(F.min(F.col("oi.unit_price")).alias("min_unit_price"))
)
