from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-06-10")
    .withColumn("quantity_rounded", F.round("quantity", 2))
)
