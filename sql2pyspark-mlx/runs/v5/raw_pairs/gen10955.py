from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select("last_restock")
    .union(df_subscriptions.select("start_date"))
    .distinct()
)
