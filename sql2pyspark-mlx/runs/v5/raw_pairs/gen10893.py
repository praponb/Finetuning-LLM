from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(~((F.col("stores.city") == "Berlin") & (F.col("stores.sqft") >= 11447)))
    .select(F.col("stores.opened_date"), F.col("orders.order_date"))
    .orderBy("order_date")
)
