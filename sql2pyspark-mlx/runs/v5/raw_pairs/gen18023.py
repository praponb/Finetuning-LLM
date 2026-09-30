from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
