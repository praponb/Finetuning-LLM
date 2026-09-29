from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 10)
    .withColumn("last_restock_minus_57d", F.date_sub("last_restock", 57))
)
