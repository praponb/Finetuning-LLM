from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 27)
    .select("store_id")
    .unionByName(df_orders.filter(F.col("channel") == "web").select("store_id"))
    .distinct()
)
