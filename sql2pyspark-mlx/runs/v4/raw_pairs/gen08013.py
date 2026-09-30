from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select("id", "store_id")
    .unionByName(df_inventory.filter(F.col("quantity") == 78).select("id", "store_id"))
)
