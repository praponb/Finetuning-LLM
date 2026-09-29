from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1010)
    .select("channel")
    .unionByName(df_orders.filter(F.col("shipping_fee").isNotNull()).select("channel"))
)
