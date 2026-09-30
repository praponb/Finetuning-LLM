from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("store_id") == 4)

summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    summary
    .select("product_id", "total_quantity", "stddev_quantity")
    .orderBy(F.col("stddev_quantity").desc(), "product_id")
    .limit(3)
)
