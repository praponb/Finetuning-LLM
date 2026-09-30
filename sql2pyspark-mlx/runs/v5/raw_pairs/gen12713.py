from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .intersect(df_subscriptions.filter(F.col("monthly_fee") > 120).select("status"))
)
