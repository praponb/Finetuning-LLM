from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .union(df_shipments.select("shipped_date"))
    .distinct()
    .orderBy("last_restock")
)
