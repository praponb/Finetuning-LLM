from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
