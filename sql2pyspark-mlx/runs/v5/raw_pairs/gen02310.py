from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select("status")
    .subtract(df_subscriptions.select("status"))
)
