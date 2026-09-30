from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & F.col("product_id").isin([3, 5, 9, 10]))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") < 70)
)
