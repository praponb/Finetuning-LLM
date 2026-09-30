from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersect(df_subscriptions.select("status"))
    .orderBy(F.col("status").desc())
)
