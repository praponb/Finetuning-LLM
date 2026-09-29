from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand").isin(["Globex", "Acme"]))
    .groupBy(
        F.when(F.col("p.brand").isin(["Acme", "Stark"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .orderBy("avg_unit_price", "brand_group")
    .limit(1)
)
