from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy(F.col("avg_quantity").desc(), "store_id")
    .limit(5)
)
