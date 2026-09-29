from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .filter(F.col("avg_quantity") > 110)
    .select("product_id", "avg_quantity", "first_last_restock")
    .orderBy(F.col("product_id").desc())
)
