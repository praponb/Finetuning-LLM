from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([24, 32]))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
)
