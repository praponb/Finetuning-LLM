from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .intersect(df_orders.select("shipping_fee"))
    .orderBy("amount")
)
