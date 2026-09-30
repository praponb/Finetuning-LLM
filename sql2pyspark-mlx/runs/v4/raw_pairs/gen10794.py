from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.col("last_restock").alias("event_date"))
    .subtract(df_stores.select(F.col("opened_date").alias("event_date")))
)
