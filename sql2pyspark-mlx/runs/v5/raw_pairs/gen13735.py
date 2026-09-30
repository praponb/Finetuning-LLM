from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 89)
    .groupBy("product_id", "store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("quantity_range") < 33)
    .select("product_id", "store_id", "quantity_range", "stddev_quantity")
    .orderBy("stddev_quantity", "product_id", "store_id")
    .limit(5)
)
