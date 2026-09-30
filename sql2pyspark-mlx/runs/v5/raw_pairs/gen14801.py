from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .groupBy(
        "store_id",
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)
