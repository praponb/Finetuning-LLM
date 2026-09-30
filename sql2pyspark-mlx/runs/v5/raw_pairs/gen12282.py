from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE", "CANCELLED"]))
    .select("start_date")
    .union(df_shipments.select("shipped_date"))
    .distinct()
)
