from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("store_id").isin([1, 4, 5]) & (F.col("quantity") < 166)) | (F.col("store_id") == 4)))
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .filter(F.col("quantity_range") < 53)
    .orderBy(F.col("product_id").desc(), "quantity_range")
)
