from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("customer_id") == 22).select("status"))
)
