from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 200)
    .orderBy(
        F.col("avg_quantity").desc(),
        F.col("total_count").desc(),
        "product_id",
        "store_id",
    )
    .limit(3)
)
