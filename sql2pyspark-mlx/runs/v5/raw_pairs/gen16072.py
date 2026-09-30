from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("last_restock_month").desc())
)
