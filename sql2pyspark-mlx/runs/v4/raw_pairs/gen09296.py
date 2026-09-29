from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-11-01")
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
    .orderBy("status")
)
