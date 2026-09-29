from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1030)
    .select("order_date")
    .unionByName(df_orders.filter(F.col("discount") <= 0.19).select("order_date"))
)
