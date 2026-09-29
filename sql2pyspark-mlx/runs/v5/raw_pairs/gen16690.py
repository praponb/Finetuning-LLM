from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .unionByName(df_shipments.select(F.col("shipped_date").alias("last_restock")))
    .distinct()
    .orderBy(F.col("last_restock").desc())
)
