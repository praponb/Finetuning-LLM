from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 9, 10]))
    .select("last_restock")
    .subtract(df_subscriptions.select("start_date"))
)
