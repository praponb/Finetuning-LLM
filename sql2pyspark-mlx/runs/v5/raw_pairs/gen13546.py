from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-11-01", "2024-09-01"))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .orderBy(F.col("store_id").desc())
)
