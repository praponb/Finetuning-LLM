from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-01-01")
    .select("delivered_date")
    .union(df_subscriptions.select("end_date"))
    .distinct()
)
