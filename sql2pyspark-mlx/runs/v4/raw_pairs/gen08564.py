from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(124, 175))
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
    .orderBy("last_restock_month")
    .limit(5)
)
