from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .union(df_subscriptions.select("start_date"))
)
