from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("last_last_restock"),
    )
)
