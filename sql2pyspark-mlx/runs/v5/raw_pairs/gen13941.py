from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .orderBy("status")
)
