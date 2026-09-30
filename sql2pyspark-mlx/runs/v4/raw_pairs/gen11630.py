from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("max_quantity") > 80)
    .orderBy(F.col("max_quantity").desc())
)
