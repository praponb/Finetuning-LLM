from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .orderBy("stddev_quantity", "last_restock_year")
)
