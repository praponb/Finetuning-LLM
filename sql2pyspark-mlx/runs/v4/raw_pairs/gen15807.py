from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "Flash")
    .select("cost")
    .union(df_orders.select("shipping_fee"))
    .distinct()
)
