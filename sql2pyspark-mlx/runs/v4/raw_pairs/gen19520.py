from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(df_subscriptions.select("start_date"))
    .distinct()
)
