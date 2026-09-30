from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "LOST")
    .select("delivered_date")
    .union(df_subscriptions.select("end_date"))
)
