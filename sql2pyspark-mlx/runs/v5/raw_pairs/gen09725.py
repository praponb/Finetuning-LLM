from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)
