from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.col("quantity").between(72, 225)).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_semi")
    .select(F.col("stores.id"), F.col("stores.sqft"), F.col("stores.opened_date"))
)
