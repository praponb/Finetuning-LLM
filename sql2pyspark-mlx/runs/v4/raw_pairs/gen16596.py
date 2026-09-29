from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.city").isin(["Berlin", "Chiang Mai"]), "tier_1").otherwise("other").alias("city_group"),
        F.col("s.country"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("total_quantity") > 780)
    .orderBy(F.col("total_quantity").desc())
)
