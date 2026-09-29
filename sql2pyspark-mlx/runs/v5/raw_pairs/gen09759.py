from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("p.cost") >= 740, "high").when(F.col("p.cost") >= 180, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc())
)
