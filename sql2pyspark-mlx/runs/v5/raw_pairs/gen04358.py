from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-07-18", "2024-09-01"))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("quantity_range") > 49)
)
