from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("sqft").between(5493, 7521)).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.year(F.col("i.last_restock")) >= 2024)
    .select(F.col("i.id"), F.col("i.last_restock"))
    .orderBy("last_restock", "id")
)
