from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
