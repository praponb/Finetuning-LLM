from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("total_count").desc())
)
