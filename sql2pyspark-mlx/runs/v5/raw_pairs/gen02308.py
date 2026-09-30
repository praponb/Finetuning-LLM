from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web", "store"]))
    .select("customer_id")
    .intersect(df_orders.filter(F.col("customer_id") == 11).select("customer_id"))
    .orderBy("customer_id")
)
