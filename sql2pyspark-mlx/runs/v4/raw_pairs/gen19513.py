from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 8]))
    .select("last_restock")
    .union(df_orders.select("order_date"))
    .distinct()
)
