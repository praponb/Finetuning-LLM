from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 228)
    .select("last_restock")
    .intersect(df_shipments.select(F.col("delivered_date").alias("last_restock")))
    .orderBy("last_restock")
)
