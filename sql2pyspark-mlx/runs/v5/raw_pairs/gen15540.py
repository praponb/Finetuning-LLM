from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("total_count") < 5)
    .orderBy(F.col("total_count").desc())
)
