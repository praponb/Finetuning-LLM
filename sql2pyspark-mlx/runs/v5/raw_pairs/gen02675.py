from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.lit("inventory").alias("source"), "last_restock")
    .union(df_stores.select(F.lit("store").alias("source"), "opened_date"))
)
