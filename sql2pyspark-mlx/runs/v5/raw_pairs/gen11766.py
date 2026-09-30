from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2023-05-01")
    .groupBy("product_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") > 140)
    .select("product_id", "last_restock_quarter", "min_quantity")
    .orderBy("product_id", "last_restock_quarter")
    .limit(50)
)
