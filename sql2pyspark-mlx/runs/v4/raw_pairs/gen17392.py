from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 2) & (F.col("store_id") == 4))
    .withColumn("last_restock_plus_75d", F.date_add("last_restock", 75))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
)
