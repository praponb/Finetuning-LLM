from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(df_accounts.select(F.col("opened_date").alias("event_date")))
    .distinct()
    .orderBy("event_date")
)
