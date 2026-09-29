from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(((F.col("product_id") == 2) & (F.col("quantity") < 226)) | F.col("store_id").isin([1, 4])))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)
