from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5]))
    .select("store_id")
    .unionByName(df_inventory.filter(F.col("quantity") == 143).select("store_id"))
    .distinct()
)
