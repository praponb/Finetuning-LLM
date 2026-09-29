from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .intersect(df_subscriptions.select(F.col("end_date").alias("last_restock")))
    .orderBy("last_restock")
)
