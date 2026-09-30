from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([2, 4, 5, 6]) & F.col("store_id").isin([1, 3, 4])))
    .select("store_id", "quantity")
)
