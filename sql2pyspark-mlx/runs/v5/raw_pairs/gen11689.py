from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .subtract(df_inventory.filter(F.col("quantity") >= 197).select("last_restock"))
)
