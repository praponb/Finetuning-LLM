from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(156, 225))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") > 9)
    .select("last_restock_quarter", "quantity_count")
    .orderBy(F.col("quantity_count").desc(), "last_restock_quarter")
    .limit(1)
)
