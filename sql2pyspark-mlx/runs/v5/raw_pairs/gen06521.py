from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 62)
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
)
