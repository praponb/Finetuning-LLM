from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .filter(F.col("inventory.last_restock").between("2023-12-08", "2024-02-25"))
    .groupBy(F.date_trunc("MONTH", F.col("stores.opened_date")).alias("opened_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("row_count") > 1)
)
