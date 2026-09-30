from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .unionByName(df_orders.select(F.col("order_date").alias("last_restock")))
    .distinct()
    .orderBy("last_restock")
)
