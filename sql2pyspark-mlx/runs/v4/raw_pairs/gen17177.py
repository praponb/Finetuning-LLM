from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country") == "UK")
    .groupBy(
        F.when(F.col("s.city").isin(["Bangkok", "Tokyo"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 45)
)
