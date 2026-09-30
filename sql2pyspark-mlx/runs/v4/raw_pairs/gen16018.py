from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-05-01")
    .select("id", "quantity", F.floor("quantity").alias("quantity_whole"))
    .orderBy(F.col("quantity").desc(), "quantity_whole")
)
