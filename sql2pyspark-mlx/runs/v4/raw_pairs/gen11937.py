from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .union(df_inventory.select("last_restock"))
    .orderBy(F.col("end_date").desc())
)
