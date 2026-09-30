from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "RETURNED"]))
    .select("channel")
    .unionByName(df_orders.filter(F.year("order_date") >= 2023).select("channel"))
)
