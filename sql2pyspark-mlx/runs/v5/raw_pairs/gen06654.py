from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 3, 4, 5]))
    .groupBy(
        F.when(F.col("p.price") >= 1010, "high").when(F.col("p.price") >= 460, "mid").otherwise("low").alias("price_bucket"),
        F.when(F.col("p.cost") >= 580, "high").when(F.col("p.cost") >= 210, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
)
