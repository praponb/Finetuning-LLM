from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(150, 227))
    .select("last_restock")
    .intersect(df_shipments.select(F.col("shipped_date").alias("last_restock")))
)
