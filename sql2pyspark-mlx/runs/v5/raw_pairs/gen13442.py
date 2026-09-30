from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .union(df_subscriptions.select("end_date"))
    .distinct()
)
