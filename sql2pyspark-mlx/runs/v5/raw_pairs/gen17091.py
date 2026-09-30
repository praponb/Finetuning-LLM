from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select("monthly_fee")
    .intersect(df_shipments.select("cost"))
)
