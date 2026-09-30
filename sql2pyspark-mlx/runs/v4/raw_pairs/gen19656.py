from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNotNull())
    .groupBy(
        F.when(F.col("p.price") >= 1130, "high").when(F.col("p.price") >= 520, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
)
