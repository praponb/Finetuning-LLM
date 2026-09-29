from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 126)
    .select("quantity")
    .unionByName(df_inventory.filter(F.col("store_id") == 3).select("quantity"))
)
