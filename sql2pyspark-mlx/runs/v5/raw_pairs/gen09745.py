from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .union(df_subscriptions.select("monthly_fee"))
    .distinct()
)
