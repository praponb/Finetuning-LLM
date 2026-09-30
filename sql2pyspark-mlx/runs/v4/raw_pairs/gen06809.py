from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2025-05-17")
    .select("status")
    .intersect(df_subscriptions.select("status"))
    .orderBy("status")
)
