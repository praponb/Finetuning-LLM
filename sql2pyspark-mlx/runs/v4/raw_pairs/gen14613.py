from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(df_inventory.select(F.col("last_restock").alias("opened_date")))
    .distinct()
)
