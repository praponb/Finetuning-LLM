from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 106) & F.col("quantity").between(133, 202) & F.col("last_restock").between("2023-06-22", "2024-02-08"))
    .groupBy("product_id", "store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.min("last_restock").alias("first_last_restock"),
    )
)
