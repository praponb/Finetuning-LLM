from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") >= 2023) & (F.col("last_restock") <= "2024-03-01"))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
)
