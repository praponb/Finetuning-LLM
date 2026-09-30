from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .groupBy(
        F.when(F.col("s.rating") >= 3.5, "high").when(F.col("s.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
    .orderBy(F.col("rating_bucket").desc())
)
