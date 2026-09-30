from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("order_date")
    .unionByName(df_orders.filter(F.col("shipping_fee") >= 18).select("order_date"))
    .distinct()
)
