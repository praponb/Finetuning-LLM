from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6]))
    .select("last_restock")
    .union(df_subscriptions.select("start_date"))
)
