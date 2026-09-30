from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([1, 4, 38]))
    .select("status")
    .intersect(df_subscriptions.select("status"))
    .orderBy(F.col("status").desc())
)
