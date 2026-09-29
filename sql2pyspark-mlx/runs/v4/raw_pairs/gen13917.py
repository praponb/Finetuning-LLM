from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-09-24")
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 4)
    .select("product_id", "cnt")
)
