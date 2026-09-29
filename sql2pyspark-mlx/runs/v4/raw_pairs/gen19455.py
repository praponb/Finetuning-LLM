from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 11, 12]))
    .select("id", "quantity")
)
