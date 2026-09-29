from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.city").isin(["Berlin", "New York", "Tokyo"]))
    .groupBy(
        F.when(F.col("s.city").isin(["Singapore", "London"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .orderBy("total_quantity")
)
