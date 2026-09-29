from pyspark.sql import functions as F

max_amount = df_orders.filter(F.col("store_id") == 36).agg(F.max("amount")).first()[0]

result = (
    df_orders
    .filter(F.col("amount") > max_amount)
    .select("id", "amount")
)
