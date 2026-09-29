from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .intersect(df_shipments.select("delivered_date"))
)
