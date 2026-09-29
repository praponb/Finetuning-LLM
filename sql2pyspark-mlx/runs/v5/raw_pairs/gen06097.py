from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([2, 4, 5, 7]) & (F.col("quantity") != 85) & F.col("last_restock").between("2023-05-01", "2023-07-01")))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
        F.max("quantity").alias("max_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") < 790)
    .drop("total_quantity")
)
