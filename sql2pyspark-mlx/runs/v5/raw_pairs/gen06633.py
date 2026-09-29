from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .union(df_inventory.filter(F.col("product_id") == 8).select("last_restock"))
    .orderBy(F.col("opened_date").desc())
)
