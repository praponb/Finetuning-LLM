from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("p.stock_qty").isNotNull())
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.sqft") >= 13714, "high").when(F.col("s.sqft") >= 5641, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy(F.col("stddev_quantity").desc())
)
