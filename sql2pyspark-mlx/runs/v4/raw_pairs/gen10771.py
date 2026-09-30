from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") <= 3.2)
    .groupBy(
        F.when(F.col("s.city").isin(["Chiang Mai", "Tokyo"]), "group_a").otherwise("other").alias("city_group"),
        F.col("s.city"),
    )
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
)
